from datetime import date

from functions import exchange_rates_api


def test_xr_get():
    symbols = ['EUR', 'PHP', 'HKD']
    response = exchange_rates_api.get(date(2020, 1, 1), base='USD', symbols=symbols)
    assert set(response.keys()) == {'rates', 'date', 'base'}
    assert set(response['rates'].keys()) == set(symbols)


def test_xr_get_history():
    symbols = ['EUR', 'PHP', 'HKD']
    start_at = date(2020, 1, 2)
    end_at = date(2020, 1, 3)
    response = exchange_rates_api.get_history(
        start_at=start_at,
        end_at=end_at,
        base='USD',
        symbols=symbols,
    )
    print(response)
    assert set(response.keys()) == {'rates', 'start_at', 'end_at', 'base'}
    assert set(response['rates'].keys()) == {start_at.isoformat(), end_at.isoformat()}
    for k, v in response['rates'].items():
        assert set(v.keys()) == set(symbols)
