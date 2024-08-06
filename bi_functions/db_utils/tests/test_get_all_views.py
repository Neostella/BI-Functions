import unittest
from bi_functions.db_utils.get_all_views import get_all_views

class TestGetAllViews(unittest.TestCase):

    def test_get_all_views(self):
        # You can add specific test cases here
        # For now, we are just checking if the function runs without errors
        try:
            get_all_views()
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"get_all_views() raised {e} unexpectedly!")

if __name__ == "__main__":
    unittest.main()