import random
import time
maxTime = 300


def t():
    global current
    global shot
    temp = time.time()
    if time.time() <= shot + 3:
        current += temp - shot
    shot = temp

def my_very_long_function_with_an_equally_very_long_name(start_time):
    print("now")
    global shot
    global current
    current = 0
    shot = time.time()
    matrices = []
    # Create an empty matrix
    matrix = []

    # Set a number of matrices to generate
    matricesToGenerate = 100

    for i in range(matricesToGenerate):
        # Create an empty row
        row = []
        if current - start_time >= maxTime:
            return
        # Iterate over the columns
        for j in range(matricesToGenerate):
            # Add a random element to the row
            if current - start_time >= maxTime:
                return

            row.append(random.randint(0, 10))

        # Add the row to the matrix
        matrix.append(row)

        matrices.append(matrix)
        result = []
    while True:
        t()
        # Iterate over the remaining matrices in the list
        for i in range(1, len(matrices)):
            # Create an empty result matrix
            if current - start_time >= maxTime:
                return
            temp = []

            # Iterate over the rows in the result matrix
            for j in range(len(result)):
                # Create an empty row
                if current - start_time >= maxTime:
                    return
                row = []
                # Iterate over the columns in the current matrix
                for k in range(len(matrices[i][0])):
                    # Initialize the dot product to 0
                    if current - start_time >= maxTime:
                        return
                    dot_product = 0

                    # Iterate over the elements in the row and column
                    for l in range(len(result[0])):
                        # Calculate the dot product
                        if current - start_time >= maxTime:
                            return
                        dot_product += result[j][l] * matrices[i][l][k]

                    # Add the dot product to the row
                    row.append(dot_product % 10)
    return


start_time = 0
my_very_long_function_with_an_equally_very_long_name(start_time)
print(current)
# Stop the timer

f = open("done.txt", "w")
f.write("The elapsed time is " + str(current) + " seconds")
f.close()