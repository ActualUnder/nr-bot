#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 OUTPUT_DIR [SERVER_DNS] [SERVER_IP] [CLIENT_CN]"
  echo "Example: $0 ./bridge-certs nr-bot 10.77.0.1 metro-bot"
}

if [[ $# -lt 1 || $# -gt 4 ]]; then
  usage >&2
  exit 2
fi

output_dir=$1
server_dns=${2:-nr-bot}
server_ip=${3:-10.77.0.1}
client_cn=${4:-metro-bot}

if [[ -z "$output_dir" || "$output_dir" == "/" || "$output_dir" == "." ]]; then
  echo "Refusing unsafe output directory: $output_dir" >&2
  exit 2
fi

if [[ -e "$output_dir" ]] && find "$output_dir" -mindepth 1 -print -quit | grep -q .; then
  echo "Output directory is not empty; refusing to overwrite certificates: $output_dir" >&2
  exit 1
fi

command -v openssl >/dev/null 2>&1 || {
  echo "openssl is required" >&2
  exit 1
}

umask 077
mkdir -p "$output_dir/authority" "$output_dir/nr-bot" "$output_dir/metro-bot"
temporary_dir=$(mktemp -d)
trap 'rm -rf "$temporary_dir"' EXIT

openssl req \
  -x509 \
  -newkey rsa:3072 \
  -sha256 \
  -nodes \
  -days 3650 \
  -subj "/CN=Metro NR private bridge CA" \
  -keyout "$output_dir/authority/ca.key" \
  -out "$output_dir/authority/ca.crt"

openssl req \
  -new \
  -newkey rsa:3072 \
  -sha256 \
  -nodes \
  -subj "/CN=$server_dns" \
  -keyout "$output_dir/nr-bot/server.key" \
  -out "$temporary_dir/server.csr"

cat >"$temporary_dir/server.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:${server_dns},IP:${server_ip}
EOF

openssl x509 \
  -req \
  -sha256 \
  -days 825 \
  -in "$temporary_dir/server.csr" \
  -CA "$output_dir/authority/ca.crt" \
  -CAkey "$output_dir/authority/ca.key" \
  -CAcreateserial \
  -extfile "$temporary_dir/server.ext" \
  -out "$output_dir/nr-bot/server.crt"

openssl req \
  -new \
  -newkey rsa:3072 \
  -sha256 \
  -nodes \
  -subj "/CN=$client_cn" \
  -keyout "$output_dir/metro-bot/client.key" \
  -out "$temporary_dir/client.csr"

cat >"$temporary_dir/client.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth
subjectAltName=DNS:${client_cn}
EOF

openssl x509 \
  -req \
  -sha256 \
  -days 825 \
  -in "$temporary_dir/client.csr" \
  -CA "$output_dir/authority/ca.crt" \
  -CAkey "$output_dir/authority/ca.key" \
  -CAserial "$output_dir/authority/ca.srl" \
  -extfile "$temporary_dir/client.ext" \
  -out "$output_dir/metro-bot/client.crt"

install -m 0644 "$output_dir/authority/ca.crt" "$output_dir/nr-bot/ca.crt"
install -m 0644 "$output_dir/authority/ca.crt" "$output_dir/metro-bot/ca.crt"
chmod 0600 \
  "$output_dir/authority/ca.key" \
  "$output_dir/nr-bot/server.key" \
  "$output_dir/metro-bot/client.key"
chmod 0644 \
  "$output_dir/authority/ca.crt" \
  "$output_dir/nr-bot/ca.crt" \
  "$output_dir/nr-bot/server.crt" \
  "$output_dir/metro-bot/ca.crt" \
  "$output_dir/metro-bot/client.crt"

openssl verify \
  -CAfile "$output_dir/authority/ca.crt" \
  "$output_dir/nr-bot/server.crt" \
  "$output_dir/metro-bot/client.crt"

echo
echo "Created:"
echo "  NR container:    $output_dir/nr-bot/{server.crt,server.key,ca.crt}"
echo "  Metro container: $output_dir/metro-bot/{client.crt,client.key,ca.crt}"
echo "  Keep offline:    $output_dir/authority/ca.key"
echo
echo "Do not copy the CA private key into either bot container."
