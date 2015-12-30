def str_to_number(s):
    try:
        return int(s)
    except ValueError:
        return float(s)
