from faker import Faker
fake = Faker()
Faker.seed(0)

with open("file.txt", "w+") as f:
    for i in range(5000):
        str = fake.sentence(nb_words=20)
        clean = str[:-1]
        f.write(clean)
        f.write("\n")
    f.close()