
if __name__ == "__main__":
    with open("nohup") as f:
        content = f.readlines()

    content = [x.strip() for x in content]

    for line in content:
        print(line)
