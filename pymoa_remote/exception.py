import traceback
from types import CodeType

__all__ = (
    'RemoteException', 'extract_frames', 'get_traceback_from_frames',
    'get_fake_traceback_obj', 'serialize_exception',
    'raise_remote_exception_from_frames')


class RemoteException(Exception):
    pass


class _DummyException(Exception):
    pass


def extract_frames(exception: BaseException) -> list:
    frames = []
    frame: traceback.FrameSummary
    for frame in traceback.StackSummary.extract(
            traceback.walk_tb(exception.__traceback__)):
        frames.append((frame.filename, frame.lineno, frame.name, frame.line))

    return frames


def get_fake_traceback_obj(filename, lineno, name, line):
    # adapted from jinja
    globals_vars = {
        "__name__": filename,
        "__file__": filename,
        "_DummyException": _DummyException,
    }
    # Raise an exception at the correct line number.
    code = compile(
        "\n" * (lineno - 1) + "raise _DummyException", filename, "exec")

    # Build a new code object that points to the template file and
    # replaces the location with a block name.

    # Collect arguments for the new code object. CodeType only
    # accepts positional arguments, and arguments were inserted in
    # new Python versions.
    code_args = []

    for attr in (
        "argcount",
        "posonlyargcount",  # Python 3.8
        "kwonlyargcount",
        "nlocals",
        "stacksize",
        "flags",
        "code",  # codestring
        "consts",  # constants
        "names",
        "varnames",
        ("filename", filename),
        ("name", name),
        "firstlineno",
        "lnotab",
        "freevars",
        "cellvars",
    ):
        if isinstance(attr, tuple):
            # Replace with given value.
            code_args.append(attr[1])
            continue

        try:
            # Copy original value if it exists.
            code_args.append(getattr(code, "co_" + attr))
        except AttributeError:
            # Some arguments were added later.
            continue

    code = CodeType(*code_args)

    # Execute the new code, which is guaranteed to raise, and return
    # the new traceback without this frame.
    tb = None
    try:
        exec(code, globals_vars)
    except _DummyException as e:
        tb = e.__traceback__.tb_next

    tb.tb_next = None
    return tb


def get_traceback_from_frames(frames):
    tracebacks = []
    for frame in frames:
        tracebacks.append(get_fake_traceback_obj(*frame))

    root = tracebacks[0]
    next_tb = root
    for tb in tracebacks[1:]:
        next_tb.tb_next = tb
        next_tb = tb

    return root


def serialize_exception(e):
    return {
        'frames': extract_frames(e),
        'err_type': e.__class__.__name__,
        'value': str(e),
    }


def raise_remote_exception_from_frames(frames=(), err_type=None, value=''):
    if err_type and value:
        msg = f'{err_type}: {value}'
    elif err_type:
        msg = f'{err_type}'
    elif value:
        msg = f'{value}'
    else:
        msg = ''

    if frames:
        raise RemoteException(msg) from RemoteException(msg).with_traceback(
            get_traceback_from_frames(frames))
    raise RemoteException(msg)
