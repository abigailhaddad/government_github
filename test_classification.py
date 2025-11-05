#!/usr/bin/env python3
"""
Test cases for IT specialist classification
"""

import unittest
from classify_it_specialist import is_it_specialist

class TestITSpecialistClassification(unittest.TestCase):
    
    def test_should_be_it_specialist(self):
        """Test cases that should be classified as IT specialist"""
        positive_cases = [
            # Basic patterns
            "IT Specialist",
            "Information Technology Specialist",
            "Senior Retail IT Specialist",
            "Information Technology Support Specialist",
            "IT Support Specialist", 
            "Information Technology Specialist (InfoSec)",
            "SUPERVISORY IT SPECIALIST (NETWORK) (TITLE 32)",
            "IT SPECIALIST",
            "IT SPECIALIST (NETWORK) (Title 32)",
            "IT SPECIALIST (SYSADMIN/NETWORK)",
            "IT SPECIALIST (POLICY AND PLANNING/ENTERPRISE ARCHITECTURE)",
            "IT SPECIALIST (CUSTSPT) (TITLE 32)",
            
            # ITSPEC patterns
            "ITSPEC (SYSADMIN) (TITLE 32)",
            "ITSPEC (INFOSEC/INFOSEC) (TITLE 32)",
            "ITSPEC (NETWORK)",
            "ITSPEC (SYSADMIN)",
            "ITSPEC (CUSTSPT)",
            
            # IT SPEC patterns  
            "IT SPEC (NETWORK)",
            "IT SPEC (SYS ADMIN) (TITLE 32)",
            "IT SPEC (SYSADMIN)",
            "SUPV IT SPEC (INFOSEC)",
            
            # Edge cases
            "Specialist in IT",
            "Information Technology Data Specialist",
            "IT Security Specialist",
            "Junior IT Specialist",
        ]
        
        for title in positive_cases:
            with self.subTest(title=title):
                self.assertTrue(is_it_specialist(title), f"'{title}' should be IT specialist")
    
    def test_should_not_be_it_specialist(self):
        """Test cases that should NOT be classified as IT specialist"""
        negative_cases = [
            # Non-specialist IT roles
            "Information Technology Manager", 
            "Computer Systems Administrator (CSA)",
            "Chief Information Officer",
            "IT Branch Manager #104",
            "Supervisory IT Project Manager (Policy and Planning)",
            "Chief Artificial Intelligence Officer",
            "Chief Technology Officer",
            "Information Technology Project Manager",
            "IT PROGRAM MANAGER (PLCYPLN/SYSANALYSIS)",
            "IT PROJECT MANAGER (APPSW)",
            "IT Program Manager (SYSANALYSIS)",
            "IT System Administrator",
            
            # Technician roles
            "Temporary Information Technology Technician II - 1 Year Term",
            "Information Technology Technician",
            
            # Other roles
            "Security Architect, CG-2210-14",
            "DAM NECK ACTIVITY INFORMATION SYSTEMS SECURITY OFFICER (ISSO)",
            "Assistant Director (Section Chief (APDB/CBASS)), CM-2210-00",
            "Informational Technology/Operational Technology (IT/OT) Cybersecurity Assessor",
            
            # Non-IT roles
            "Human Resources Specialist",
            "Accountant",
            "Program Manager",
            "Software Engineer",
            
            # Edge cases
            "IT Department Head",
            "Information Technology Director",
            "",
            None,
        ]
        
        for title in negative_cases:
            with self.subTest(title=title):
                self.assertFalse(is_it_specialist(title), f"'{title}' should NOT be IT specialist")
    
    def test_case_insensitive(self):
        """Test that classification is case insensitive"""
        cases = [
            "it specialist",
            "IT SPECIALIST", 
            "It Specialist",
            "iT sPeCiAlIsT",
            "itspec",
            "ITSPEC",
            "ItSpec",
        ]
        
        for title in cases:
            with self.subTest(title=title):
                self.assertTrue(is_it_specialist(title), f"'{title}' should be IT specialist (case insensitive)")

if __name__ == "__main__":
    unittest.main()