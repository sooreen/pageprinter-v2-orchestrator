"""Online Markdown editor for project_info.md."""

import html as html_module
from pathlib import Path as FilePath

from fastapi import APIRouter, Path
from fastapi.responses import HTMLResponse

from app import storage

router = APIRouter()


def _load_default_template() -> str:
    """Load the default template from the single source of truth."""
    candidates = [
        FilePath("/app/templates/project_info.md"),
        FilePath(__file__).resolve().parents[3] / "templates" / "project_info.md",
    ]
    for p in candidates:
        if p.exists():
            return p.read_text(encoding="utf-8")
    # Fallback if template file not found
    return "# Описание проекта\n\n## Название\n\n## Тематика\n\n## Целевая аудитория\n"


def _build_html(project_id: str, content: str) -> str:
    escaped = html_module.escape(content)
    esc_id = html_module.escape(project_id)
    return f'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>project_info.md — {esc_id}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#1a1a2e;color:#e0e0e0}}
.header{{background:#16213e;padding:12px 24px;display:flex;align-items:center;justify-content:space-between}}
.header h1{{font-size:16px;color:#a0c4ff}}
.header .proj{{color:#7b8cde;font-size:14px}}
.wrap{{display:flex;height:calc(100vh - 48px)}}
.pane{{flex:1;padding:16px;overflow-y:auto}}
.pane-edit{{border-right:1px solid #2a2a4a}}
textarea{{width:100%;height:calc(100% - 48px);background:#0f0f23;color:#e0e0e0;border:1px solid #2a2a4a;border-radius:4px;padding:12px;font-family:'JetBrains Mono','Fira Code',monospace;font-size:14px;line-height:1.6;resize:none}}
textarea:focus{{outline:none;border-color:#4a6fa5}}
.pane-preview{{background:#0f0f23;padding:16px 24px}}
.pane-preview h1{{color:#a0c4ff;margin:16px 0 8px;font-size:22px}}
.pane-preview h2{{color:#7b8cde;margin:16px 0 8px;font-size:18px;border-bottom:1px solid #2a2a4a;padding-bottom:4px}}
.pane-preview p{{margin:8px 0;line-height:1.6}}
.pane-preview ul{{margin:8px 0 8px 24px}}
.pane-preview li{{margin:4px 0}}
.pane-preview a{{color:#4a9eff}}
.btn{{padding:8px 24px;border:none;border-radius:4px;cursor:pointer;font-size:14px;font-weight:500}}
.btn-save{{background:#2d6a4f;color:#fff}}
.btn-save:hover{{background:#40916c}}
.btn-save:disabled{{background:#555;cursor:not-allowed}}
.bar{{display:flex;gap:8px;align-items:center;margin-bottom:8px}}
.lbl{{font-size:12px;color:#888;text-transform:uppercase;letter-spacing:1px}}
.status{{font-size:12px;color:#888}}
.status.ok{{color:#40916c}}
.status.err{{color:#e74c3c}}
</style>
</head>
<body>
<div class="header"><h1>project_info.md</h1><span class="proj">{esc_id}</span></div>
<div class="wrap">
 <div class="pane pane-edit">
  <div class="bar">
   <span class="lbl">Редактор</span>
   <button class="btn btn-save" id="saveBtn" onclick="save()">Сохранить</button>
   <span class="status" id="st"></span>
  </div>
  <textarea id="ed" oninput="preview()">{escaped}</textarea>
 </div>
 <div class="pane pane-preview" id="pv"><span class="lbl">Превью</span></div>
</div>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/dompurify/dist/purify.min.js"></script>
<script>
function preview(){{document.getElementById('pv').innerHTML='<span class="lbl">Превью</span>'+DOMPurify.sanitize(marked.parse(document.getElementById('ed').value))}}
async function save(){{
 const b=document.getElementById('saveBtn'),s=document.getElementById('st');
 b.disabled=true;s.textContent='Сохранение...';s.className='status';
 try{{
  const r=await fetch('/projects/{esc_id}/files/project_info.md',{{method:'PUT',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{content:document.getElementById('ed').value}})}});
  if(r.ok){{s.textContent='Сохранено';s.className='status ok'}}
  else{{const d=await r.json();s.textContent='Ошибка: '+(d.detail||r.statusText);s.className='status err'}}
 }}catch(e){{s.textContent='Сеть: '+e.message;s.className='status err'}}
 b.disabled=false
}}
preview()
</script>
</body></html>'''


@router.get("/projects/{project_id}/editor", response_class=HTMLResponse)
def editor(project_id: str = Path(pattern=r"^[a-z0-9][a-z0-9_-]*$")):
    """Serve the Markdown editor for project_info.md."""
    path = f"{project_id}/project_info.md"
    try:
        content = storage.read_text(path)
    except Exception as e:
        error_str = str(e)
        if "NoSuchKey" in error_str or "NoSuchBucket" in error_str:
            content = _load_default_template()
        else:
            from fastapi import HTTPException
            raise HTTPException(
                status_code=502,
                detail=f"Cannot read from storage: {error_str}",
            )
    return HTMLResponse(_build_html(project_id, content))
