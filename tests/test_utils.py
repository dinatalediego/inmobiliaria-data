from inmobiliario_scrapers.core.utils import fix_mojibake, parse_money_to_float, parse_float


def test_fix_mojibake():
    assert fix_mojibake("NÃ¡poles").startswith("N")


def test_parse_money():
    assert parse_money_to_float("Desde S/ 327,550") == 327550.0


def test_parse_float():
    assert parse_float("57.04 m²") == 57.04
    assert parse_float("44,63 mÂ²") == 44.63
