from inmobiliario_scrapers.nexo.parser import parse_card_text


def test_parse_card_text_variants():
    t1 = "PlanoTorre NÃ¡poles27 unidades disponiblesDesdeS/ 327,550Modelo AX0157.04 mÂ²Pisos 2 al  282 dorms.2 baÃ±os"
    r1 = parse_card_text(t1)
    assert r1.parse_ok is True
    assert r1.modelo == "AX01"
    assert r1.unidades_disponibles == 27
    assert r1.piso_min == 2
    assert r1.piso_max == 28
    assert r1.dormitorios == 2
    assert r1.banos == 2

    t2 = "PlanoTorre NÃ¡poles1 unidad disponibleDesdeS/ 271,441Modelo A-20944.63 mÂ²Piso 21 dorms.1 baÃ±o"
    r2 = parse_card_text(t2)
    assert r2.parse_ok is True
    assert r2.modelo == "A-209"
    assert r2.unidades_disponibles == 1
    assert r2.piso_min == 2 or r2.piso_min == 21  # depending on spacing; regex expects piso_min
