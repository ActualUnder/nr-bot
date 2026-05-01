[Unit]
Description=Metro T3 Learner Discord Bot
After=network-online.target
Wants=network-online.target

[Service]
User=metrobot
Group=metrobot
WorkingDirectory=/opt/metro-t3-discord-bot
EnvironmentFile=/etc/metro-bot.env
ExecStart=/usr/local/bin/metro-t3-learner-start.sh
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
