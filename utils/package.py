from typing import Optional

import pkg_resources


def get_package_version() -> Optional[str]:
    try:
        return pkg_resources.get_distribution('datametry').version
    except Exception:
        pass

    return None