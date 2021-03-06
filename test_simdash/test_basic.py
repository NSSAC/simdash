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
    assert b"<title>Simdash Visualization Dashboard</title>" in response.data
