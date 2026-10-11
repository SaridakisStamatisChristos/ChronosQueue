from __future__ import annotations
import unittest
from chronosqueue.errors import InvalidTransition
from chronosqueue.state import STATES, ensure_transition

class StateMachineCase(unittest.TestCase):
    def test_every_state_is_known(self):
        self.assertEqual({"scheduled","ready","leased","succeeded","failed","dead-lettered","cancelled"}, set(STATES))
    def test_normal_and_admin_boundaries(self):
        ensure_transition("ready","leased")
        ensure_transition("leased","succeeded")
        with self.assertRaises(InvalidTransition): ensure_transition("succeeded","ready")
        with self.assertRaises(InvalidTransition): ensure_transition("cancelled","ready")
        ensure_transition("cancelled","ready",administrative=True)
        ensure_transition("dead-lettered","scheduled",administrative=True)
    def test_unknown_states_rejected(self):
        with self.assertRaises(InvalidTransition): ensure_transition("ready","mystery")

if __name__=='__main__': unittest.main()
