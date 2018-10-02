import operator
lines = [[0, 123, 1231, 32, 12], [0, 123, 1231, 432, 12]]

merged = []
for i in range(0, len(lines)):
  start = 1538030992;
  fg = False
  for j in range(0, len(lines[i])):
    if not fg:
      start = start + lines[i][j]
    else:
      end = start + lines[i][j]
      merged.append([start, end])
      start = end
    fg = not fg
    
    
new = sorted(merged, key=operator.itemgetter(0, 1))
print new

start_interval = 0
end_interval = 0
final = []

while i < len(new):
  interval = new[i]
  end_interval = interval[1]  
  start_interval = interval[0]
  for j in range(i + 1, len(new)):
    if end_interval < new[j][1]:
      if end_interval < new[j][0]:
        break
      else:
        end_interval = new[j][1]
    i = j  
  final.append([start_interval, end_interval])
  i = i + 1 
print final
  
          
    
