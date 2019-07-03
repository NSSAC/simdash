"""
Basic tests for simdash.
"""

from simdash.serve import app

def test_route_():
    """
    Test the root route.
    """

    client = app.test_client()

    response = client.get("/")
    assert response.status_code == 200
    assert "<title>Embedding Vega-Lite</title>" in response.data
