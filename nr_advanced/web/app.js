"use strict";
const $ = id => document.getElementById(id);
const NS = "http://www.w3.org/2000/svg";
let data = null, area = "T3", selected = null, positions = new Map(), nodeEls = new Map();
let view = {x: 0, y: 0, w: 1200, h: 650}, layoutKey = "", lastLayoutArea = "", lastEvent = null, lost = false;
let polling = false, trainRequest = 0;
const dt = new Intl.DateTimeFormat("en-GB", {timeZone:"Europe/London", hour:"2-digit",minute:"2-digit",second:"2-digit"});
const fullDt = new Intl.DateTimeFormat("en-GB", {timeZone:"Europe/London",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"});
const timeText = v => v ? fullDt.format(new Date(v*1000)) : "Not received";
const ago = s => s < 60 ? `${Math.round(s)}s ago` : s < 3600 ? `${Math.floor(s/60)}m ago` : `${Math.floor(s/3600)}h ago`;
function el(tag, text, cls) {const e = document.createElement(tag); if(text != null) e.textContent=text; if(cls) e.className=cls; return e;}
function svg(tag, attrs={}) {const e=document.createElementNS(NS,tag); for(const [k,v] of Object.entries(attrs)) e.setAttribute(k,v); return e;}
function label(n) {return [...new Set(n.labels.map(r=>r.description))].join(" / ");}
function empty(target, title, text) {target.replaceChildren(); const p=el("div",null,"empty-state");p.append(el("strong",title),el("p",text));target.append(p);}
function setView() {$('map').setAttribute('viewBox',`${view.x} ${view.y} ${view.w} ${view.h}`);}
function visibleNodes() {return data ? data.nodes.filter(n=>area==='all'||n.area===area) : [];}
function fit() {
  const points=visibleNodes().map(n=>positions.get(n.id)).filter(Boolean);
  if(!points.length) return;
  const xs=points.map(p=>p.x),ys=points.map(p=>p.y);
  const box=$('map').getBoundingClientRect(), ratio=box.width/box.height || 2;
  const xmin=Math.min(...xs)-100,xmax=Math.max(...xs)+100,ymin=Math.min(...ys)-90,ymax=Math.max(...ys)+90;
  const w=Math.max(xmax-xmin,(ymax-ymin)*ratio,750),h=w/ratio;
  view={x:(xmin+xmax-w)/2,y:(ymin+ymax-h)/2,w,h};setView();
}
function zoom(factor,cx=view.x+view.w/2,cy=view.y+view.h/2) {
  const w=Math.min(20000,Math.max(350,view.w*factor)),ratio=w/view.w;
  view={x:cx-(cx-view.x)*ratio,y:cy-(cy-view.y)*ratio,w,h:view.h*ratio};setView();
}
function focusNode(n) {
  if(area!=='all'&&area!==n.area){area=n.area;layoutKey='';render();}
  const p=positions.get(n.id);if(!p)return;
  const rect=$('map').getBoundingClientRect(),w=Math.max(450,Math.min(1400,rect.width*1.15)),h=w*rect.height/rect.width;
  view={x:p.x-w/2,y:p.y-h/2,w,h};setView();selectNode(n);
}

// Lay out actual links with a deterministic breadth-first spanning graph.
// This is deliberately a topological schematic, never a geographic map.
function graphLayout(nodes,edges,startY=150) {
  const ids=new Set(nodes.map(n=>n.id)),adj=new Map(nodes.map(n=>[n.id,new Set()]));
  edges.forEach(e=>{const a=e.area+':'+e.origin,b=e.area+':'+e.destination;if(ids.has(a)&&ids.has(b)){adj.get(a).add(b);adj.get(b).add(a);}});
  const output=new Map(),left=new Set([...ids].sort());let componentY=startY;
  while(left.size){
    const ordered=[...left].sort((a,b)=>(adj.get(a).size===1?-1:0)-(adj.get(b).size===1?-1:0)||a.localeCompare(b));
    const queue=[[ordered[0],0]],levels=new Map();left.delete(ordered[0]);
    for(let i=0;i<queue.length;i++){
      const [id,depth]=queue[i];if(!levels.has(depth))levels.set(depth,[]);levels.get(depth).push(id);
      for(const next of [...adj.get(id)].sort())if(left.delete(next))queue.push([next,depth+1]);
    }
    let height=120;
    for(const [depth,level] of levels){level.forEach((id,i)=>output.set(id,{x:100+depth*150,y:componentY+i*92}));height=Math.max(height,level.length*92);}
    componentY+=height+100;
  }
  return output;
}
function buildLayout() {
  const key=area+'|'+data.nodes.map(n=>n.id).sort().join(',');
  if(key===layoutKey)return false;
  layoutKey=key;positions=new Map();$('area-labels').replaceChildren();let offset=0;
  const areas=area==='all'?data.areas:data.areas.filter(a=>a.id===area);
  for(const a of areas){
    const nodes=data.nodes.filter(n=>n.area===a.id),fixed=nodes.filter(n=>n.position),rest=nodes.filter(n=>!n.position);
    let bottom=offset+160;
    for(const n of fixed){positions.set(n.id,{x:n.position[0]+100,y:n.position[1]+offset});bottom=Math.max(bottom,n.position[1]+offset);}
    const computed=graphLayout(rest,data.edges,fixed.length?bottom+180:offset+150);
    computed.forEach((p,id)=>positions.set(id,p));
    const caption=svg('text',{x:75,y:offset+35,class:'area-title'});caption.textContent=`${a.id} / ${a.name.toUpperCase()}${nodes.length?'':' · WAITING FOR DATA'}`;$('area-labels').append(caption);
    offset=Math.max(bottom,...[...computed.values()].map(p=>p.y),offset+150)+240;
  }
  return true;
}
function drawMap() {
  const changed=buildLayout(),nodes=visibleNodes(),occupied=$('occupied-only').checked;
  $('empty-map').hidden=nodes.length>0;
  $('edges').replaceChildren();$('nodes').replaceChildren();nodeEls=new Map();
  const edgeMap=new Map();
  for(const e of data.edges){if(area!=='all'&&e.area!==area)continue;const k=e.area+':'+e.origin+'>'+e.destination;
    if(!edgeMap.has(k)||e.source==='observed CA')edgeMap.set(k,e);}
  edgeMap.forEach(e=>{
    const a=positions.get(e.area+':'+e.origin),b=positions.get(e.area+':'+e.destination);if(!a||!b)return;
    const path=svg('path',{d:`M ${a.x} ${a.y} L ${(a.x+b.x)/2} ${a.y} L ${(a.x+b.x)/2} ${b.y} L ${b.x} ${b.y}`,class:`edge ${e.source==='observed CA'?'':'reference'}`});
    const title=svg('title');title.textContent=`${e.area}:${e.origin} → ${e.destination} · ${e.source} · ${e.observations} observations`;path.append(title);$('edges').append(path);
  });
  for(const n of nodes){
    if(occupied&&!n.headcode)continue;
    const p=positions.get(n.id);if(!p)continue;
    const live=n.live&&!lost,cls=n.headcode?(live?'live':'unconfirmed'):'';
    const g=svg('g',{transform:`translate(${p.x} ${p.y})`,class:`node ${cls} ${n.ambiguous?'ambiguous':''} ${selected===n.id?'selected':''}`,tabindex:0,role:'button','aria-label':`${n.id} ${n.headcode||'no confirmed occupation'} ${label(n)}`});
    g.append(svg('rect',{x:-34,y:-15,width:68,height:30}));
    const code=svg('text',{'text-anchor':'middle',y:4});code.textContent=n.headcode||n.berth;g.append(code);
    if(n.headcode){const bid=svg('text',{'text-anchor':'middle',y:31});bid.textContent=n.berth;g.append(bid);}
    const names=label(n);if(names&&!names.toLowerCase().startsWith('between')){const t=svg('text',{'text-anchor':'middle',y:-28,class:'place'});t.textContent=(n.ambiguous?'⚠ ':'')+names.slice(0,28);g.append(t);}
    const title=svg('title');title.textContent=`${n.id} · ${names||'Unmapped location'}\n${n.headcode||'Empty or not yet observed'} · ${n.headcode?(live?'Live':'Unconfirmed'):''}\n${timeText(n.event_at)}`;g.append(title);
    g.addEventListener('click',()=>selectNode(n));g.addEventListener('keydown',e=>{if(e.key==='Enter'){selectNode(n);e.stopPropagation();}});
    $('nodes').append(g);nodeEls.set(n.id,g);
  }
  if(changed&&lastLayoutArea!==area){
    const centre=area==='T3'?positions.get('T3:6242'):null;
    if(centre){const r=$('map').getBoundingClientRect(),w=Math.max(450,Math.min(1400,r.width*1.15)),h=w*r.height/r.width;view={x:centre.x-w/2,y:255-h/2,w,h};setView();}
    else fit();
    lastLayoutArea=area;
  }
  const currentArea=data.areas.find(a=>a.id===area);
  $('area-caption').textContent=area==='all'?'All configured areas · separate schematic panels':`${area} · ${currentArea?currentArea.name:''}`;
  animateSteps();
}
function animateSteps(){
  const events=data.events.filter(e=>lastEvent!==null&&e.id>lastEvent&&e.kind==='CA'&&data.generated_at-e.event_at<12);
  if(data.events.length)lastEvent=Math.max(...data.events.map(e=>e.id));
  if(matchMedia('(prefers-reduced-motion: reduce)').matches||lost)return;
  for(const e of events.slice(-12)){
    if(area!=='all'&&e.area!==area)continue;
    const a=positions.get(e.area+':'+e.origin),b=positions.get(e.area+':'+e.destination);if(!a||!b)continue;
    const g=svg('g',{class:'train-motion'}),circle=svg('circle',{r:7}),text=svg('text',{x:12,y:-12});text.textContent=e.headcode;g.append(circle,text);$('animations').append(g);
    const start=performance.now();function frame(t){const k=Math.min(1,(t-start)/800);g.setAttribute('transform',`translate(${a.x+(b.x-a.x)*k} ${a.y+(b.y-a.y)*k})`);if(k<1)requestAnimationFrame(frame);else g.remove();}requestAnimationFrame(frame);
  }
}
function renderTabs(){
  $('area-tabs').replaceChildren();
  for(const a of [{id:'all',name:'All areas'},...data.areas]){const b=el('button',a.id==='all'?'All':a.id,a.id===area?'active':'');b.title=a.name;b.setAttribute('aria-pressed',String(a.id===area));b.onclick=()=>{area=a.id;layoutKey='';render();};$('area-tabs').append(b);}
}
function renderFindings(){
  const target=$('findings');target.replaceChildren();
  if(!data.findings.length){empty(target,'Building the evidence','No supported slow-running observations in the last hour. Timings need repeated TD steps or paired TRUST reports.');return;}
  for(const f of data.findings.slice(0,6)){
    const card=el('div',null,'finding'),d=f.details;card.append(el('span',f.kind==='td_slow'?'SLOWER BERTH MOVEMENT':'TIME LOST AGAINST PLAN','tag'),el('h3',`${f.headcode} · ${f.section}`),el('p',d.summary));
    if(d.restriction_matches.length){const r=d.restriction_matches[0];card.append(el('p',`Possible contributor: ${r.PassengerSpeed||'?'} mph TSR at ${r.FromLocation}. ${r.Reason||''}`));}
    else card.append(el('p','Cause unknown · no current matching TSR in received data.'));
    card.append(el('p',timeText(f.event_at),'subtle'));target.append(card);
  }
}
function renderRestrictions(){
  const target=$('restrictions');target.replaceChildren();
  if(!data.restrictions.length){empty(target,'No applicable local snapshot entries','This does not mean the line is unrestricted. Check source health below; TSR data is published weekly.');return;}
  for(const r of data.restrictions.slice(0,8)){
    const card=el('div',null,'restriction'),speed=el('div',`${r.PassengerSpeed||'?'} `,'speed');speed.append(el('span',`mph passenger / ${r.FreightSpeed||'?'} freight`));
    card.append(speed,el('h3',r.FromLocation===r.ToLocation?r.FromLocation:`${r.FromLocation} → ${r.ToLocation}`),el('p',`${r.LineName||'Line not specified'} · ${r.Direction||'Direction not supplied'} · ${r.Reason||'Reason not supplied'}`),el('p',`${r.publication} · published ${timeText(r.published_at)}`,'subtle'),el('p','Location match. Exact line/mileage overlap and effect on a train are unconfirmed.','subtle'));target.append(card);
  }
}
function renderFeeds(){
  $('feeds').replaceChildren();for(const f of data.feeds){const card=el('div',null,'feed-item'),good=!lost&&['live','loaded','weekly snapshot available','connected (event driven)'].includes(f.state);
    card.append(el('strong',f.feed.toUpperCase()),el('span',lost?'View disconnected':f.state,'feed-state'+(good?' good':'')),el('p',`Received ${timeText(f.received_at)}`));
    if(f.error)card.append(el('p',f.error));if(f.rejected)card.append(el('p',`${f.rejected} rejected items`));$('feeds').append(card);}
}
function render(){
  if(!data)return;
  $('demo-banner').hidden=!data.demo;
  $('logout').hidden=!data.auth_required;
  const live=data.nodes.filter(n=>n.headcode&&n.live&&!lost).length,unconfirmed=data.nodes.filter(n=>n.headcode).length-live;
  $('live-count').textContent=live;$('unconfirmed-count').textContent=`${unconfirmed} unconfirmed occupations`;$('berth-count').textContent=data.nodes.length;
  $('finding-count').textContent=data.findings.length;$('restriction-count').textContent=data.restrictions.length;$('baseline-count').textContent=`${data.baseline_samples.toLocaleString()} timing samples`;
  $('coverage').textContent=data.coverage_note;$('updated').textContent=lost?'Connection lost · occupations unconfirmed':`Updated ${dt.format(new Date(data.generated_at*1000))}`;
  const liveFeeds=data.feeds.filter(f=>f.feed.startsWith('td:')&&f.state==='live').length;
  $('connection').textContent=lost?'VIEW DISCONNECTED':data.demo?'DEMO MODE':liveFeeds?`${liveFeeds} TD AREAS LIVE`:'AWAITING LIVE DATA';
  $('connection').className='live-badge'+(!lost&&liveFeeds&&!data.demo?' good':'');
  renderTabs();drawMap();renderFindings();renderRestrictions();renderFeeds();
  if(selected){const n=data.nodes.find(n=>n.id===selected);if(n){const status=$('selected-state');if(status)status.textContent=n.headcode?`${n.headcode} · ${n.live&&!lost?'Live':'Unconfirmed'} · ${ago(Math.max(0,Date.now()/1000-n.event_at))}`:'No current occupation reported';}}
}
async function selectNode(n){
  selected=n.id;for(const [id,g]of nodeEls)g.classList.toggle('selected',id===selected);
  const target=$('selection');target.replaceChildren(el('span','BERTH / TRAIN DETAIL','eyebrow'),el('h3',`${n.id} · ${label(n)||'Location not mapped'}`));
  const state=el('p',n.headcode?`${n.headcode} · ${n.live&&!lost?'Live':'Unconfirmed'} · ${ago(n.age_seconds||0)}`:'No current occupation reported','muted');state.id='selected-state';target.append(state);
  if(n.ambiguous)target.append(el('p','Conflicting descriptions in the supplied catalogue. Location needs review.','source-note'));
  target.append(el('p','Sources: '+([...new Set(n.labels.map(x=>x.source))].join(', ')||'Observed TD ID; geographic position unknown'),'source-note'));
  if(!n.headcode){target.append(el('p','An empty or unseen berth is not proof that the track is clear.','muted'));return;}
  const req=++trainRequest;
  try{
    const response=await fetch('/api/train/'+encodeURIComponent(n.headcode));if(!response.ok)throw Error('Train details unavailable');
    const d=await response.json();if(req!==trainRequest||selected!==n.id)return;
    const trail=el('div',null,'trail');d.events.slice(0,10).reverse().forEach(e=>trail.append(el('span',`${e.area}:${e.destination||e.origin} ${e.kind}`)));target.append(trail,el('p',d.identity_note,'muted'));
    if(!d.runs.length){target.append(el('p','No candidate TRUST activation has been received. Metro movements may have no matching TRUST schedule.','source-note'));return;}
    for(const r of d.runs.slice(0,4)){const card=el('div',null,'report');card.append(el('h3',`${r.train_id} · ${r.uid} · ${r.status}`),el('p',`Origin ${timeText(r.origin_at)} · candidate match`));
      const reports=el('ul');for(const e of r.reports.slice(0,5)){const m=e.details,delay=m.delay_minutes;reports.append(el('li',`${timeText(e.event_at)} · ${e.location_names.join(' / ')||e.stanox||'No location'} · ${m.event_type||e.kind}${delay==null?'':` · ${delay>0?'+':''}${delay} min`}`));}card.append(reports);
      for(const s of r.schedules.slice(0,3))card.append(el('p',`${s.source.toUpperCase()} ${s.stp} · ${s.start_date} to ${s.end_date}`,'muted'));target.append(card);}
  }catch(e){if(selected===n.id)target.append(el('p',e.message,'source-note'));}
}
async function poll(){
  if(polling)return;polling=true;
  try{
    const response=await fetch('/api/snapshot',{signal:AbortSignal.timeout(10000)});
    if(response.status===401){if(!$('login-dialog').open)$('login-dialog').showModal();lost=true;render();return;}
    if(!response.ok)throw Error('Snapshot unavailable');
    data=await response.json();lost=false;render();
  }catch(e){lost=true;render();if(!data){$('connection').textContent='VIEW DISCONNECTED';$('updated').textContent='Cannot reach the map server';}}
  finally{polling=false;setTimeout(poll,2000);}
}
$('login-form').addEventListener('submit',async e=>{e.preventDefault();$('login-error').textContent='';try{const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:$('token').value})});if(!r.ok)throw Error('That access key was not accepted.');$('token').value='';$('login-dialog').close();}catch(e){$('login-error').textContent=e.message;}});
$('login-dialog').addEventListener('cancel',e=>e.preventDefault());
$('logout').onclick=async()=>{await fetch('/api/logout',{method:'POST'});data=null;selected=null;lastEvent=null;$('nodes').replaceChildren();$('selection').replaceChildren();if(!$('login-dialog').open)$('login-dialog').showModal();};
$('search').addEventListener('input',()=>{const q=$('search').value.trim().toLowerCase(),target=$('search-results');target.replaceChildren();target.hidden=!q||!data;if(target.hidden)return;
  const matches=data.nodes.filter(n=>`${n.id} ${n.headcode||''} ${label(n)}`.toLowerCase().includes(q)).slice(0,12);if(!matches.length)target.append(el('p','No matching berth received.','empty-state'));
  for(const n of matches){const b=el('button',`${n.id} ${n.headcode||''} · ${label(n)||'Unmapped'}`);b.onclick=()=>{focusNode(n);target.hidden=true;$('search').value='';};target.append(b);}});
$('search').addEventListener('keydown',e=>{if(e.key==='Escape')$('search-results').hidden=true;if(e.key==='Enter'){const first=$('search-results').querySelector('button');if(first)first.click();}});
$('occupied-only').onchange=drawMap;$('zoom-in').onclick=()=>zoom(.75);$('zoom-out').onclick=()=>zoom(1.33);$('fit').onclick=fit;
let drag=null,dragged=false;
$('map').addEventListener('pointerdown',e=>{if(e.target.closest('.node'))return;drag={x:e.clientX,y:e.clientY,v:{...view}};dragged=false;$('map').setPointerCapture(e.pointerId);$('map').classList.add('dragging');});
$('map').addEventListener('pointermove',e=>{if(!drag)return;const r=$('map').getBoundingClientRect(),dx=e.clientX-drag.x,dy=e.clientY-drag.y;dragged=Math.abs(dx)+Math.abs(dy)>5;view.x=drag.v.x-dx/r.width*drag.v.w;view.y=drag.v.y-dy/r.height*drag.v.h;setView();});
function endDrag(){drag=null;$('map').classList.remove('dragging');}
$('map').addEventListener('pointerup',endDrag);$('map').addEventListener('pointercancel',endDrag);
$('map').addEventListener('wheel',e=>{e.preventDefault();const r=$('map').getBoundingClientRect();zoom(e.deltaY>0?1.12:.89,view.x+(e.clientX-r.left)/r.width*view.w,view.y+(e.clientY-r.top)/r.height*view.h);},{passive:false});
$('map').addEventListener('keydown',e=>{const step=view.w*.1;if(e.key==='+')zoom(.8);else if(e.key==='-')zoom(1.25);else if(e.key==='ArrowLeft')view.x-=step;else if(e.key==='ArrowRight')view.x+=step;else if(e.key==='ArrowUp')view.y-=step;else if(e.key==='ArrowDown')view.y+=step;else return;e.preventDefault();setView();});
let previousWidth=$('map').getBoundingClientRect().width;
window.addEventListener('resize',()=>{const r=$('map').getBoundingClientRect(),cx=view.x+view.w/2,cy=view.y+view.h/2,w=Math.max(450,view.w*r.width/previousWidth),h=w*r.height/r.width;previousWidth=r.width;view={x:cx-w/2,y:cy-h/2,w,h};setView();});
$('clock').textContent=dt.format(new Date());
setInterval(()=>{$('clock').textContent=dt.format(new Date());if(data&&Date.now()/1000-data.generated_at>15&&!lost){lost=true;render();}},1000);
poll();
