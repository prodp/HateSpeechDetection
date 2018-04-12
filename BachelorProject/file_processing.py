import os


def mkdirs_if_not_exist(folder):
    """Create the full directory if it doesn't exist

    Args:
        folder:

    Returns:

    """
    if not os.path.exists(folder):
        os.makedirs(folder)
