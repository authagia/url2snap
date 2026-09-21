from snapcast.meta_url2snap import SnapcastMetadataBridge


def test_snapcast_metadata_includes_duration_and_original_url():
    url = "https://www.youtube.com/watch?v=abc123"
    result = SnapcastMetadataBridge._snap_metadata(
        {
            "title": "Song",
            "artist": "Artist",
            "album": "Album",
            "duration": 123.4,
        },
        url,
    )

    assert result["url"] == url
    assert result["duration"] == 123.4
