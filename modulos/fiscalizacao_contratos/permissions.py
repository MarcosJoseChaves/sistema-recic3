"""Regras de acesso centralizadas do módulo."""

from functools import wraps

from flask import abort, jsonify
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


def admin_json_required(view_function):
    """Exige administrador e mantém erros de APIs no formato JSON."""

    @wraps(view_function)
    def protected_view(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({"error": "Autenticação necessária."}), 401
        if getattr(current_user, "role", None) != "admin":
            return jsonify({"error": "Acesso não autorizado para este recurso."}), 403
        return view_function(*args, **kwargs)

    return protected_view
