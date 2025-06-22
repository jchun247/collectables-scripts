def format_basep(series, number):
    """Formats 'basep' sets. Returns the number as-is."""
    return str(number)

def format_np(series, number):
    """Formats 'np' sets. Pads the number with leading zeros to three digits."""
    return f"{int(number):03d}"

def format_modern_promo(series, number):
    """Formats modern promo sets ('swshp', 'svp'). Prepends the series and pads the number."""
    return f"{series.upper()}{int(number):03d}"

def format_other_promo(series, number):
    """Formats all other promo sets. Prepends the series without padding the number."""
    return f"{series.upper()}{number}"

PROMO_SET_RULES = {
    "basep": format_basep,
    "np": format_np,
    "swshp": format_modern_promo,
    "svp": format_modern_promo,
    "default": format_other_promo,
}