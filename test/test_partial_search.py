import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Add parent dir to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock dependencies
sys.modules['requests'] = MagicMock()

# Create Dummy Base Peer
class DummyBasePeer:
    def __init__(self, *args, **kwargs):
        self.storage = MagicMock()
        self.ring = MagicMock()
        self.self_id = args[0] if len(args) > 0 else "peer:50000"
        self.known_peers = args[1] if len(args) > 1 else []
        
    def _search_local_storage(self, query):
        return [] # Default empty local results

    def _resolve_conflicts(self, results):
        return results # Pass-through for simple testing

# Mock modules
sys.modules['base'] = MagicMock()
sys.modules['base'].BasePeer = DummyBasePeer

# Import peers
import peer.naive as naive_module

# FIX: metadata.py expects 'import naive', so we must alias it
sys.modules['naive'] = naive_module

import peer.metadata as metadata_module

class TestPartialSearch(unittest.TestCase):
    
    def setUp(self):
        # Setup Request Mock
        self.mock_requests = sys.modules['requests']
        self.mock_requests.get.side_effect = None # Reset side effects

    def test_naive_partial_availability(self):
        """
        Test FAULT TOLERANCE in NaivePeer (Flooding).
        Scenario: 1 Neighbor Alive, 1 Neighbor Dead.
        Expected: partial_result=True, results from Alive neighbor.
        """
        p = naive_module.NaivePeer("self:5000", ["alive:5000", "dead:5000"])
        
        # Mock behavior
        def side_effect(url, params, timeout):
            if "alive" in url:
                # Returns success
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                mock_resp.json.return_value = {"results": [{"filename": "B.txt", "host": "alive"}]}
                return mock_resp
            else:
                # Raises Timeout/Error
                raise Exception("Network Timeout")
        
        self.mock_requests.get.side_effect = side_effect
        
        # Execute Search
        response = p.search({"q": "test"})
        
        # Verify
        self.assertTrue(response["partial_result"], "Should be partial because 'dead' node failed")
        self.assertEqual(len(response["results"]), 1, "Should have 1 result from 'alive' node")
        self.assertEqual(response["results"][0]["filename"], "B.txt")
        print("\n✅ NaivePeer Partial Search Passed")

    def test_metadata_partial_availability(self):
        """
        Test FAULT TOLERANCE in MetadataPeer (Scatter-Gather).
        Scenario: Shard 0 Alive, Shard 1 Dead in a 2-shard system.
        Expected: partial_result=True, results from Shard 0.
        """
        p = metadata_module.MetadataPeer("self:5000", [])
        p.INDEX_SHARDS = 2
        
        # Mock Ring to convert hash -> node_id
        p.ring.get_node.side_effect = lambda h: "shard0" if int(h[-1], 16) % 2 == 0 else "shard1"
        
        # Mock Remote Fetch directly to avoid requests complexity in threads
        # We patch _fetch_remote_index instead of requests because threading makes mocks tricky
        original_fetch = p._fetch_remote_index
        
        p._fetch_remote_index = MagicMock()
        def fetch_side_effect(node, key):
            if "shard1" in str(node) or "shard1" in key:
                return None # Simulate FAILURE (which returns None now)
            else:
                return [{"filename": "Shard0_File.txt"}] # Success
                
        p._fetch_remote_index.side_effect = fetch_side_effect
        
        # Execute Search
        # Note: We need a query that triggers scatter-gather
        response = p.search({"genre": "action"})
        
        # Verify
        self.assertTrue(response["partial_result"], "Should be partial because Shard 1 failed")
        self.assertGreaterEqual(len(response["results"]), 1, "Should have results from Shard 0")
        self.assertEqual(response["results"][0]["filename"], "Shard0_File.txt")
        print("\n✅ MetadataPeer Partial Search Passed")

if __name__ == '__main__':
    unittest.main()
