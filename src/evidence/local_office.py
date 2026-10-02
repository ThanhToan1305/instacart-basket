"""Install a user-local LibreOffice by extracting official apt packages, no sudo."""
import subprocess,re,json,os,shutil,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];BASE=ROOT/'outputs/tools/libreoffice';DEBS=BASE/'debs';INSTALL=BASE/'root'

def main():
    DEBS.mkdir(parents=True,exist_ok=True);INSTALL.mkdir(exist_ok=True)
    if '--debug' in sys.argv:
        missing=[name for name in ['strace','libunwind8'] if not list(DEBS.glob(name+'_*.deb'))]
        if missing:subprocess.run(['apt-get','download',*missing],cwd=DEBS,check=True)
        for name in ['strace','libunwind8']:
            subprocess.run(['dpkg-deb','-x',str(next(DEBS.glob(name+'_*.deb'))),str(INSTALL)],check=True)
        program=INSTALL/'usr/lib/libreoffice/program'
        env=dict(os.environ,LD_LIBRARY_PATH=str(INSTALL/'usr/lib/x86_64-linux-gnu')+':'+str(program))
        subprocess.run([str(INSTALL/'usr/bin/strace'),'-f','-o',str(BASE/'debug.log'),str(program/'soffice.bin'),'--headless','--version'],env=env)
        return
    pending=['libreoffice-writer'];seen=set();needed=[]
    while pending:
        package=pending.pop()
        if package in seen:continue
        seen.add(package)
        status=subprocess.run(['dpkg-query','-W','-f=${db:Status-Status}',package],capture_output=True,text=True)
        if status.returncode==0 and status.stdout=='installed':continue
        policy=subprocess.run(['apt-cache','policy',package],capture_output=True,text=True).stdout
        if not re.search(r'Candidate: (?!\(none\))\S+',policy):continue
        needed.append(package)
        dependencies=subprocess.run(['apt-cache','depends',package],capture_output=True,text=True,check=True).stdout
        for line in dependencies.splitlines():
            match=re.match(r'\s*(?:PreDepends|Depends): ([a-zA-Z0-9.+-]+)$',line)
            if match:pending.append(match[1])
    (BASE/'packages.json').write_text(json.dumps(needed,indent=2)+'\n')
    print('Official packages to extract:',len(needed),flush=True)
    subprocess.run(['apt-get','download',*needed],cwd=DEBS,check=True)
    for path in DEBS.glob('*.deb'):subprocess.run(['dpkg-deb','-x',str(path),str(INSTALL)],check=True)
    for path in (INSTALL/'usr/lib/libreoffice/program').glob('*rc'):
        text=path.read_text()
        for prefix in ['file:///usr/','file:///etc/']:
            text=text.replace(prefix,'file://'+str(INSTALL)+prefix[7:])
        path.write_text(text)
    config=INSTALL/'etc/libreoffice/registry/main.xcd'
    config.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(INSTALL/'usr/lib/libreoffice/share/.registry/main.xcd',config)
    for path in INSTALL.rglob('*'):
        if path.is_symlink():
            destination=os.readlink(path)
            if destination.startswith('/') and (INSTALL/destination.lstrip('/')).exists():
                path.unlink();path.symlink_to(INSTALL/destination.lstrip('/'))
    program=INSTALL/'usr/lib/libreoffice/program';libs=INSTALL/'usr/lib/x86_64-linux-gnu'
    (libs/'unorc').write_text((program/'unorc').read_text().replace('${ORIGIN}',program.as_uri()))
    for path in program.glob('*.so'):
        target=libs/path.name
        if not target.exists() and not target.is_symlink():target.symlink_to(path)
    env=dict(os.environ,LD_LIBRARY_PATH=str(INSTALL/'usr/lib/x86_64-linux-gnu')+':'+str(INSTALL/'usr/lib/libreoffice/program'),URE_BOOTSTRAP='vnd.sun.star.pathname:'+str(INSTALL/'usr/lib/libreoffice/program/fundamentalrc'))
    binary=INSTALL/'usr/lib/libreoffice/program/soffice'
    subprocess.run([str(binary),'--headless','--version'],env=env,check=True)
    print('Local LibreOffice ready:',binary)
if __name__=='__main__':main()
