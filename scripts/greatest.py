import greatest

# Example input with some None values
input_data = [
    [1, 2, None],
    [4, None, 6],
    [7, 5, 9]
]

# Run the greatest query
result = greatest.run_greatest_query(input_data)
print("Greatest Int:", result)
# Output: Greatest Int: [Some(7), Some(5), Some(9)]
