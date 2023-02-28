''' Text utils. '''

def decapitalize(text: str, /):
    return text[0].lower() + text[1:]


def humanize_list(x: list, /, last_separator='and'):
    if len(x) == 0:
        raise ValueError()
    if len(x) == 1:
        return x[0]
    return ', '.join(map(str, x[:-1])) + f' {last_separator} {x[-1]}'


def abbreviate_name(name: str, /) -> str:
    words = name.split(' ')
    if len(words) > 1:
        name = words[0] + ' ' + ''.join(word[0].upper() + '.' for word in words[1:])
    return name


def wrap_text_middle(x: str, /, width: int = 16) -> str:
    if len(x) <= width:
        return x
    k1 = width // 2 - 1
    k2 = width // 2 - 1 if width % 2 else width // 2 - 2
    if x[k1 - 1] == ' ':
        k1 -= 1
        k2 += 1
    elif x[-k2] == ' ':
        k1 += 1
        k2 -= 1
    return x[:k1] + '...' + x[-k2:]


def humanize_league_count(n: int, /) -> str:
    return '1 league' if n == 1 else f'{n:,} leagues'


def humanize_match_count(n: int, /) -> str:
    return '1 match' if n == 1 else f'{n:,} matches'


def humanize_season_count(n: int, /) -> str:
    return '1 season' if n == 1 else f'{n:,} seasons'
