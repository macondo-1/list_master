def test_modules_lists_and_utilities_import_cleanly():
    import modules.lists  # noqa: F401
    import modules.utilities  # noqa: F401
