"""Speaking prompts database for practice sessions."""

import random

PROMPTS = [
    # Daily life
    {"id": "daily-1", "category": "Daily Life", "text": "Describe your morning routine in detail. What do you do from the moment you wake up?", "difficulty": "beginner"},
    {"id": "daily-2", "category": "Daily Life", "text": "Tell me about the best meal you've had recently. What made it special?", "difficulty": "beginner"},
    {"id": "daily-3", "category": "Daily Life", "text": "Describe your ideal weekend. How would you spend your time?", "difficulty": "beginner"},

    # Opinions
    {"id": "opinion-1", "category": "Opinions", "text": "Do you think remote work is better than working in an office? Explain your view.", "difficulty": "intermediate"},
    {"id": "opinion-2", "category": "Opinions", "text": "What's a popular opinion that you disagree with? Why?", "difficulty": "intermediate"},
    {"id": "opinion-3", "category": "Opinions", "text": "Should social media have age restrictions? Share your thoughts.", "difficulty": "intermediate"},

    # Storytelling
    {"id": "story-1", "category": "Storytelling", "text": "Tell me about a time you were completely surprised by something.", "difficulty": "intermediate"},
    {"id": "story-2", "category": "Storytelling", "text": "Describe a challenging situation you faced and how you dealt with it.", "difficulty": "intermediate"},
    {"id": "story-3", "category": "Storytelling", "text": "Tell me about someone who has had a big influence on your life.", "difficulty": "intermediate"},

    # Abstract / Advanced
    {"id": "abstract-1", "category": "Abstract", "text": "If you could change one thing about how the education system works, what would it be and why?",  "difficulty": "advanced"},
    {"id": "abstract-2", "category": "Abstract", "text": "How do you think artificial intelligence will change daily life in the next 10 years?", "difficulty": "advanced"},
    {"id": "abstract-3", "category": "Abstract", "text": "What does success mean to you? Has your definition changed over time?", "difficulty": "advanced"},

    # Professional
    {"id": "prof-1", "category": "Professional", "text": "Explain what you do for work to someone who knows nothing about your field.", "difficulty": "intermediate"},
    {"id": "prof-2", "category": "Professional", "text": "Describe a project you're proud of and what you learned from it.", "difficulty": "intermediate"},
    {"id": "prof-3", "category": "Professional", "text": "If you were giving advice to someone starting in your field, what would you tell them?", "difficulty": "advanced"},
]


def get_random_prompt(difficulty: str | None = None, category: str | None = None) -> dict:
    """Get a random speaking prompt, optionally filtered."""
    filtered = PROMPTS
    if difficulty:
        filtered = [p for p in filtered if p["difficulty"] == difficulty]
    if category:
        filtered = [p for p in filtered if p["category"].lower() == category.lower()]

    if not filtered:
        filtered = PROMPTS

    return random.choice(filtered)


def list_categories() -> list[str]:
    """Get all available prompt categories."""
    return sorted(set(p["category"] for p in PROMPTS))
