import re

def format_gallery_card_number(set_id, set_number, total_cards):
    """
    Formats card numbers for gallery sets (e.g., 'swsh12tg').
    Example format: TG01/TG30 or GG01/GG70
    """
    # Extract the gallery prefix ('tg' or 'gg') from the set_id
    match = re.search(r'(tg|gg)$', set_id, re.IGNORECASE)
    if not match:
        return None # Should not happen if called correctly
    
    prefix = match.group(1).upper()

    # Extract the numeric part of the card number
    num_match = re.search(r'\d+', str(set_number))
    if not num_match:
        return None # Card number is not in the expected format
        
    number = int(num_match.group(0))
    
    # Format with 2-digit padding
    formatted_number = f"{number:02d}"
    
    return f"{prefix}{formatted_number}/{prefix}{total_cards}"
