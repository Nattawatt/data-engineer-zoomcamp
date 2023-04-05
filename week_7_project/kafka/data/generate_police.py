import random
import json
from faker import Faker

fake = Faker()

# Define the possible police ranks and their corresponding ages in police job
police_ranks = {
    'Chief': (30, 60),
    'Deputy Chief': (45, 55),
    'Captain': (40, 50),
    'Lieutenant': (35, 45),
    'Sergeant': (30, 40),
    'Detective': (25, 35),
    'Police Officer': (20, 30)
}

# Define the rank probabilities
rank_probabilities = {
    'Chief': 0.03,
    'Deputy Chief': 0.03,
    'Captain': 0.04,
    'Lieutenant': 0.10,
    'Sergeant': 0.25,
    'Detective': 0.25,
    'Police Officer': 0.30
}

# Generate fake data for 200 police officers
data = []
for uid in range(1, 201):
    # Randomly choose a first and last name
    first_name = fake.first_name()
    last_name = fake.last_name()
    
    # Generate a random age and age in police job
    age = random.randint(22, 55)
    age_in_police_job = age - 21
    
    # Determine the police rank based on rank probabilities
    rank = None
    ranks_with_probability = []
    for police_rank, probability in rank_probabilities.items():
        if random.random() <= probability:
            ranks_with_probability.append(police_rank)
    
    # If there is only one rank with probability, assign it to the police officer
    if len(ranks_with_probability) == 1:
        rank = ranks_with_probability[0]
    elif len(ranks_with_probability) > 1:
        # Otherwise, assign the rank randomly based on its probability
        total_probability = sum([rank_probabilities[r] for r in ranks_with_probability])
        rank_probabilities_normalized = [(r, rank_probabilities[r] / total_probability) for r in ranks_with_probability]
        rank_probabilities_normalized.sort(key=lambda x: x[1], reverse=True)
        rank = random.choices([x[0] for x in rank_probabilities_normalized], [x[1] for x in rank_probabilities_normalized])[0]
    else:
        # If there are no ranks with probability, assign 'Police Officer' by default
        rank = 'Police Officer'
    
    # Append the fake data for this police officer to the list
    plolice_dict = {"uid": uid,
                    "first_name": first_name,
                    "last_name": last_name,
                    "age": age,
                    "age_in_police_job": age_in_police_job,
                    "rank": rank
                    }
       
    data.append(plolice_dict)

# Dump locations list in JSON format
with open("polices.json", "w") as f:
    json.dump(data, f, indent=2)
