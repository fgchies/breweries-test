import responses
from breweries_pipeline.clients.openbrewery import OpenBreweryClient

@responses.activate
def test_iter_breweries_paginates_until_empty():
    base = "https://api.openbrewerydb.org/v1/breweries"

    responses.add(responses.GET, base, json=[{"id":"1"}], status=200)
    responses.add(responses.GET, base, json=[{"id":"2"}], status=200)
    responses.add(responses.GET, base, json=[], status=200)  # stop

    c = OpenBreweryClient()
    pages = list(c.iter_breweries(per_page=50, start_page=1))
    assert len(pages) == 2
    assert pages[0][0]["id"] == "1"
    assert pages[1][0]["id"] == "2"