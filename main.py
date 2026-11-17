
"""Render launcher for FEYZULLAH application."""
from importlib.util import spec_from_file_location
from pathlib import Path
import sys

filename = "FEYZULLAH_GLOBAL_VIDEO_V34_4_TEK_DOSYA(1).py"
app_file = Path(__file__).with_name(filename)

spec = spec_from_file_location("feyzullah_application", app_file)

if spec is None or spec.loader is None:
    raise RuntimeError(f"Dosya yüklenemedi: {app_file}")

module = __import__("types").ModuleType(spec.name)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

if not hasattr(module, "app"):
    raise RuntimeError("Ana dosyada app tanımı bulunamadı.")

app = module.app
