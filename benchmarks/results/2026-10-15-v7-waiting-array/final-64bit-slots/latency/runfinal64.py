from pathlib import Path
import subprocess, time, json, os, re
root=Path(__file__).resolve().parent
import hashlib,platform

out=root/'evidence-final64'; out.mkdir(exist_ok=True)
(out/'environment.txt').write_text(subprocess.run(['bash','-c','date -u +%FT%TZ; go version; uptime; lscpu | grep -E "Model name|^CPU\\(s\\)"; sha256sum final64-native.test final64-race.test review_perf_test.go'],capture_output=True,text=True,cwd=root).stdout)
rows=[]
variants=['final64']
for test in ['TestR1CancellationRegime','TestR1GuardDrain']:
  for race in [False,True]:
    for procs,n,load in [(1,128,0),(1,512,0),(16,512,0),(64,512,0),(16,128,16),(64,512,64)]:
      if test=='TestR1GuardDrain' and load: continue
      slow=set()
      for sample in range(8 if (test=='TestR1CancellationRegime' and load==64) else 3):
        order=variants[sample:]+variants[:sample]
        for variant in order:
          if variant in slow: continue
          binary=root/f'{variant}-{"race" if race else "native"}.test'
          cmd=['taskset','-c','0-15',str(binary),'-test.run',f'^{test}$','-test.v','-test.count=1','-test.timeout=15s']
          env=os.environ|{'R1_PROCS':str(procs),'R1_WAITERS':str(n),'R1_LOAD':str(load),'GORACE':'atexit_sleep_ms=0'}
          p=subprocess.run(cmd,capture_output=True,text=True,env=env)
          o=p.stdout+p.stderr
          m=re.search(r'elapsed_ns=(\d+)',o)
          ms=round(int(m.group(1))/1e6,3) if m else None
          rows.append(dict(test=test,variant=variant,race=race,procs=procs,n=n,load=load,sample=sample,exit=p.returncode,ms=ms))
          if p.returncode: slow.add(variant)
          (out/'runs.json').write_text(json.dumps(rows,indent=1)+'\n')
summary={}
for r in rows:
  k=(r['test'].replace('TestR1',''),'race' if r['race'] else 'native',r['procs'],r['n'],r['load'])
  summary.setdefault(k,{}).setdefault(r['variant'],[]).append(r['ms'] if r['ms'] is not None else 'TIMEOUT')
lines=[]
for k,v in summary.items():
  lines.append(f"{k[0]:20s} {k[1]:6s} P={k[2]:<3d} N={k[3]:<4d} load={k[4]:<3d} | "+" | ".join(f"{var}: {v.get(var,[])}" for var in variants))
(out/'summary.txt').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines))
