"""
    Modulo con los decoradores empleados en la aplicacion.
"""

def try_n_times(n=10):
    """Decorador para que una funcion se ejecute hasta n veces mientras devuelva distinto de 200."""

    def decorator(func):

        def wrapper(*args, **kwargs):

            result = None
            cont   = 0
            while result is None and cont < n:
                output = func(*args, **kwargs)
                result = output if output.status_code == 200 else None
                cont += 1

            return result

        return wrapper

    return decorator