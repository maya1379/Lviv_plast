import random
from locust import HttpUser, task, between

class VoterUser(HttpUser):
    # Пауза між запитами одного користувача від 1 до 3 секунд
    wait_time = between(1, 3)

    @task(3)
    def check_active_poll(self):
        """Імітація регулярного опитування активного голосування кожні кілька секунд"""
        self.client.get("/api/poll/active")

    @task(1)
    def cast_vote(self):
        """Спроба відправити голос із випадковим номером учасника"""
        voter_no = random.randint(1, 400)
        choice = random.choice(["За", "Проти", "Утримався"])
        
        # Замініть ID опитування, якщо воно створене у вас в системі
        payload = {
            "pollId": "active",  # або фактичний ID вашого створеного опитування
            "voterNumber": voter_no,
            "choice": choice
        }
        self.client.post("/api/vote", json=payload)
        