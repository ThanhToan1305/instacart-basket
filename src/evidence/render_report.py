"""Render DOCX with user-local LibreOffice; inspect actual PDF geometry/pages."""
import os,subprocess,json
from pathlib import Path
import pymupdf
ROOT=Path(__file__).resolve().parents[2]

def main():
    install=ROOT/'outputs/tools/libreoffice/root';program=install/'usr/lib/libreoffice/program'
    runtime=ROOT/'logs/.lo_runtime';runtime.mkdir(exist_ok=True,mode=0o700)
    cache=ROOT/'logs/.lo_cache';cache.mkdir(exist_ok=True)
    env=dict(os.environ,LD_LIBRARY_PATH=str(install/'usr/lib/x86_64-linux-gnu')+':'+str(program),
             SAL_USE_VCLPLUGIN='svp',GSETTINGS_BACKEND='memory',XDG_RUNTIME_DIR=str(runtime),XDG_CACHE_HOME=str(cache))
    env.pop('URE_BOOTSTRAP',None)
    target=ROOT/'docs/Bao_cao_prototype_hoan_thien.docx'
    command=[str(program/'soffice'),'-env:UserInstallation='+ (ROOT/'logs/.lo_profile').as_uri(),
             '--headless','--convert-to','pdf:writer_pdf_Export','--outdir',str(ROOT/'docs'),str(target)]
    result=subprocess.run(command,env=env,cwd=ROOT,capture_output=True,text=True,check=True,timeout=120)
    (ROOT/'logs/word_render.log').write_text(result.stdout+'\n'+result.stderr)
    pdf=target.with_suffix('.pdf');doc=pymupdf.open(pdf)
    previews=ROOT/'outputs/evidence/report_pages';previews.mkdir(exist_ok=True)
    bounds=[]
    for index,page in enumerate(doc):
        page.get_pixmap(matrix=pymupdf.Matrix(1,1)).save(previews/f'page_{index+1:02d}.png')
        for block in page.get_text('dict')['blocks']:
            rect=pymupdf.Rect(block['bbox'])
            if not page.rect.contains(rect):bounds.append({'page':index+1,'bbox':list(rect)})
    validation=json.loads((ROOT/'outputs/metrics/word_validation.json').read_text())
    validation.update(pdf=str(pdf),rendered_pages=len(doc),pdf_out_of_page_blocks=bounds,
                      pdf_nonempty_pages=all(bool(p.get_text().strip()) for p in doc),pdf_render='LibreOffice 26.2.5.2; every page rasterized by PyMuPDF')
    validation['status']='PASS' if 12<=len(doc)<=18 and not bounds and validation['pdf_nonempty_pages'] else 'FAIL'
    (ROOT/'outputs/metrics/word_validation.json').write_text(json.dumps(validation,indent=2)+'\n')
    print('PDF pages:',len(doc),'overflow blocks:',len(bounds),'status:',validation['status'])

if __name__=='__main__':main()
