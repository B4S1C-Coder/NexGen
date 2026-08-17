import pytest
from rag.src.preprocessor import Preprocessor

def test_slack_disentanglement():
    preprocessor = Preprocessor()
    
    # Slack thread with a clear resolution keyword
    slack_text = (
        "User1: We are seeing high latency on the payments API.\n"
        "User2: I am investigating it now.\n"
        "User1: Any updates?\n"
        "User2: Yes, I fixed the issue by increasing the timeout in the DB pool."
    )
    
    problem, resolution = preprocessor.disentangle(slack_text)
    
    assert "payments API" in problem
    assert "investigating it now" in problem
    assert "fixed the issue" in resolution
    assert "payments API" not in resolution
    
    # Slack thread without a clear resolution keyword (fallback)
    slack_text_no_res = (
        "User1: Just letting you know there's a typo on the homepage.\n"
        "User2: Will look into it later."
    )
    
    problem2, resolution2 = preprocessor.disentangle(slack_text_no_res)
    
    # It should put everything in resolution so it's not lost
    assert problem2 == ""
    assert "typo on the homepage" in resolution2
