from transformations.xr import flatten_response


def test_flatten_response():
    example_response = {
        'rates': {
            '2021-03-12': {'EUR': 0.838012235, 'HKD': 7.7635129473, 'PHP': 48.4136428392}
        },
        'start_at': '2021-03-12',
        'base': 'USD',
        'end_at': '2021-03-13',
    }
    assert flatten_response(example_response) == [{'EUR': 0.838012235, 'HKD': 7.7635129473, 'PHP': 48.4136428392, 'base': 'USD', 'date': '2021-03-12'}]
