from pathlib import Path
from breweries_pipeline.bronze.ingest import write_bronze

def test_write_bronze_writes_file(monkeypatch, tmp_path: Path):

    class DummyClient:
        def iter_breweries(self, *args, **kwargs):
            yield [{"id": "1", "nome": "AAA"}]
            yield [{"id": "2", "nome": "BBB"}]

    from breweries_pipeline import bronze
    import breweries_pipeline.bronze.ingest as ingest_mod

    monkeypatch.setattr(ingest_mod, "OpenBreweryClient", lambda: DummyClient())

    out = write_bronze(data_dir=tmp_path, per_page=2, max_pages=2)
    assert out.exists()
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2