from pathlib import Path
import shutil


def pytest_sessionstart(session):
    root = Path(session.config.rootpath)
    fixtures = root / "tests" / "fixtures" / "wheels"
    wheels = sorted(fixtures.glob("*.whl"))
    if not wheels:
        raise RuntimeError(f"Required Fabric wheel test fixtures are missing: {fixtures}")

    output = root / "dist"
    output.mkdir(exist_ok=True)
    for wheel in wheels:
        shutil.copy2(wheel, output / wheel.name)
