import nox
import os
from pathlib import Path
import shutil

WHEEL_DIR = Path(os.environ.get("CARGO_TARGET_DIR", 'target')) / "wheels"

Path('dist').mkdir(exist_ok=True)


@nox.session(python=['3.10', '3.11', '3.12', '3.13', '3.14', '3.14t', '3.15', '3.15t'])
def build_wheel(session):
    session.install('maturin')
    session.run('cargo', 'clean', external=True)
    session.run('maturin', 'build', '--interpreter', 'python', '--release')

    wheels = list(WHEEL_DIR.glob('*.whl'))
    assert len(wheels) == 1
    wheel = str(wheels[0])

    # quick test of the wheel
    session.install(wheel)
    session.run('python', '-c', 'import lazrs')

    # Save the wheel as its going to be erased by the next cargo clean
    shutil.copy(wheel, 'dist')
