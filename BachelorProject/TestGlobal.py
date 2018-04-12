myGlobal = 5

ids = set()
iteration = True
dico = {}

def func1():
    global myGlobal
    myGlobal = 42
    global ids
    ids.add("hesysdad")
    global iteration
    if(iteration):
        ids = set()
        ids.add("kajsdkjaldaj")
        iteration = False
    print(ids)

def func2():
    print(myGlobal)
    print(ids)

def func3():
    global dico
    result = dico.get("hey")
    print(result)
    if result is None:
        dico["hey"] = 1
    else:
        dico["hey"] += 1
    print(dico)

func1()
func2()
func1()
func3()
func3()