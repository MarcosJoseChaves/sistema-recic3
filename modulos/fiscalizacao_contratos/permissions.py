"""Regras de acesso centralizadas do módulo."""

from functools import wraps

from flask import abort
from flask_login import current_user, login_required


def admin_required(view_function):
    """Exige login e libera o módulo somente para administradores."""

    @login_required
    @wraps(view_function)
    def protected_view(*args, **kwargs):
        if getattr(current_user, "role", None) != "admin":
            abort(403)
        return view_function(*args, **kwargs)

    return protected_view
