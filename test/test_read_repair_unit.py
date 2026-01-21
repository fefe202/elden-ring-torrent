import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Add parent dir to path to allow importing peer modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock dependencies before importing NaivePeer
sys.modules['requests'] = MagicMock()
sys.modules['base'] = MagicMock()

# Create a dummy base class
class DummyBasePeer:
    def __init__(self, *args, **kwargs):
        self.storage = MagicMock()
        self.ring = MagicMock()
        self.self_id = args[0] if args else "peer:50000"
        self.known_peers = []

# Mock the module import so NaivePeer sees our DummyBasePeer
sys.modules['base'] = MagicMock()
sys.modules['base'].BasePeer = DummyBasePeer

import peer.naive as naive_module

class TestReadRepair(unittest.TestCase):
    def setUp(self):
        # Instantiate NaivePeer (it will use DummyBasePeer)
        self.peer = naive_module.NaivePeer(
            self_id="test_node:5000", 
            known_peers=[], 
            isp="isp", 
            region="region"
        )
        
        # Mock _send_manifest to track repairs
        # Since _send_manifest is defined in NaivePeer, we can mock it on the instance
        self.peer._send_manifest = MagicMock()

    def test_no_conflict(self):
        """Scenario: 1 file, 1 version. No repair needed."""
        results = [{
            "filename": "A.txt", 
            "host": "peer1:5000", 
            "updated_at": 100,
            "manifest": {"filename": "A.txt", "updated_at": 100}
        }]
        
        resolved = self.peer._resolve_conflicts(results)
        
        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0]["host"], "peer1:5000")
        self.peer._send_manifest.assert_not_called()

    def test_conflict_remote_newer(self):
        """Scenario: Remote version is newer than another remote version."""
        results = [
            {
                "filename": "A.txt", 
                "host": "peer1:5000", 
                "updated_at": 100, # OLD
                "manifest": {"id": "v1"} 
            },
            {
                "filename": "A.txt", 
                "host": "peer2:5000", 
                "updated_at": 200, # NEW
                "manifest": {"id": "v2"}
            }
        ]
        
        resolved = self.peer._resolve_conflicts(results)
        
        # Should return only the winner
        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0]["host"], "peer2:5000")
        self.assertEqual(resolved[0]["updated_at"], 200)
        
        # Should repair peer1
        self.peer._send_manifest.assert_called_once_with("peer1:5000", {"id": "v2"})
        print("\n Correctly triggered repair for peer1 (Stale v1 < v2)")

    def test_conflict_multiple_files(self):
        """Scenario: Multiple files, some with conflicts."""
        results = [
            # File A: No conflict
            {"filename": "A.txt", "host": "p1", "updated_at": 10, "manifest": {}},
            
            # File B: Conflict (p3 wins)
            {"filename": "B.txt", "host": "p2", "updated_at": 10, "manifest": {}}, 
            {"filename": "B.txt", "host": "p3", "updated_at": 20, "manifest": {"winner": True}}
        ]
        
        resolved = self.peer._resolve_conflicts(results)
        
        self.assertEqual(len(resolved), 2) # A and B
        
        # Check B winner
        b_res = next(r for r in resolved if r["filename"] == "B.txt")
        self.assertEqual(b_res["host"], "p3")
        
        # Check Repair
        self.peer._send_manifest.assert_called_once_with("p2", {"winner": True})
        print("\n Correctly repaired B.txt on p2")

if __name__ == '__main__':
    unittest.main()
