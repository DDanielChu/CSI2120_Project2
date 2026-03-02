/*
Student Names: Daniel Chu, Joshua Vanderlaan
Student Numbers: 300430501, 300430437
*/

package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// REMOVE FOR SINGLE THREADED
var mu sync.Mutex

// The Resident data type
type Resident struct {
	residentID     int
	firstname      string
	lastname       string
	rol            []string // resident rank order list
	matchedProgram string   // will be "" for unmatched resident
	rolIndex       int
}

// The Program data type
type Program struct {
	programID  string
	name       string
	nPositions int   // number of positions available (quota)
	rol        []int // program rank order list
	// TO ADD: a data structure for the selected resident IDs
	selectedResidents Heap
}

type Heap struct {
	theListHeap []int
}

// A function for the heap where it has three scenarios
// 1. The resident isn't on the program's ROL meaning that they won't get accepted and won't be pushed onto the heap
// 2. If the resident is on the program's ROL then:
// 2.1 If the quota hasn't reached its max, then it will push the resident into the heap and rearrange the tree so that the worst ranking student is at the top
// 2.2 If the quota has reached the max, then it will compare the current worst student and the new resident and see which one is worse and push in the new resident if it's better
func (h *Heap) push(resident *Resident, program *Program) (int, bool) {

	residentID := resident.residentID

	//If the resident isn't in the program's ROL then they aren't accepted
	if !slices.Contains(program.rol, residentID) {
		return 0, false
	}

	//If there is available quota
	if program.nPositions > len(h.theListHeap) {

		//Gets the index of where the new resident will be at
		currentIndex := len(h.theListHeap)
		var parent int
		var worseRank int
		h.theListHeap = append(h.theListHeap, residentID)

		//Does an up-heap and compares the child to its parents
		for {

			parent = h.theListHeap[int((currentIndex-1)/2)]

			//Whoever has the worse rank will be given to worseRank
			worseRank = compareTwoRanks(parent, residentID, program.rol)

			//If the worseRank was the residentID then they will switch with the parent, otherwise, it will break the while loop
			if worseRank != parent {
				h.theListHeap[currentIndex] = parent
				currentIndex = int((currentIndex - 1) / 2)
				h.theListHeap[currentIndex] = residentID
			} else {
				break
			}

		}

		resident.matchedProgram = program.programID

		return 0, false

	} else {
		//If the quota has been reached

		//Gets the lowest ranked resident which is at the top of the heap
		currentLowestResident := h.theListHeap[0]

		//Compares the top of the heap with the new resident
		currentLowestResident = compareTwoRanks(currentLowestResident, residentID, program.rol)

		//The lowest id is the previous top of the heap

		//If the resident is higher on the ROL then the current worst person in the heap
		if currentLowestResident != residentID {
			h.theListHeap[0] = residentID

			h.downHeap(0, program, len(h.theListHeap))

			return currentLowestResident, true
		}

		return 0, false

	}

}

// A function that compares two residentIDs and see which one is lower ranked on the program's ROL
func compareTwoRanks(residentID1 int, residentID2 int, programRol []int) int {

	//Goes through the entire programROl and checks which value appears first. Then it returns the other value since it was lower ranked
	for _, value := range programRol {
		if residentID1 == value {
			return residentID2
		}

		if residentID2 == value {
			return residentID1
		}

	}

	//Based on our push method, this return will never happen since the residentID1 and residentID2 will always be in the program's ROL
	return 0

}

// Returns the first item at the top of the heap
func (h *Heap) pop(program *Program) int {

	//If the heap is size 0, then it will return 0
	if len(h.theListHeap) == 0 {
		return 0
	}

	//Switches the first value with the last value
	temp := h.theListHeap[0]
	currentSize := len(h.theListHeap)
	h.theListHeap[0] = h.theListHeap[currentSize-1]
	h.theListHeap[currentSize-1] = temp

	//Does the downheap
	h.downHeap(0, program, len(h.theListHeap)-1)

	h.theListHeap = h.theListHeap[:len(h.theListHeap)-1]

	return temp

}

func (h *Heap) downHeap(currentIndex int, program *Program, currentSize int) {

	currentIndex = 0
	var leftIndex int
	var rightIndex int
	var smallest int

	for {

		leftIndex = currentIndex*2 + 1
		rightIndex = currentIndex*2 + 2
		smallest = currentIndex

		if leftIndex < currentSize && compareTwoRanks(h.theListHeap[leftIndex], h.theListHeap[smallest], program.rol) == h.theListHeap[leftIndex] {
			smallest = leftIndex
		}

		//Whoever is the worse ranked resident, they will be compared to their parent to see if they need to
		if rightIndex < currentSize && compareTwoRanks(h.theListHeap[rightIndex], h.theListHeap[smallest], program.rol) == h.theListHeap[rightIndex] {
			smallest = rightIndex
		}

		if smallest == currentIndex {
			break
		}

		h.theListHeap[currentIndex], h.theListHeap[smallest] = h.theListHeap[smallest], h.theListHeap[currentIndex]

		currentIndex = smallest

	}

}

func (h *Heap) peek() (int, bool) {
	if len(h.theListHeap) == 0 {
		return 0, false
	}

	return h.theListHeap[0], true
}

//--------------------------------------------------------------------------------------------------------------

// Parse a resident's ROL
func parseRol(s string) []string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	for i, part := range parts {
		parts[i] = strings.TrimSpace(part)
	}
	return parts
}

// Parse a program's ROL
func parseIntRol(s string) []int {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if s == "" {
		return []int{}
	}
	parts := strings.Split(s, ",")
	var ints []int
	for _, part := range parts {
		pid, _ := strconv.Atoi(strings.TrimSpace(part))
		ints = append(ints, pid)
	}
	return ints
}

// ReadCSV reads a CSV file into a map of Resident
func ReadResidentsCSV(filename string) (map[int]*Resident, error) {

	// map to store residents by ID
	residents := make(map[int]*Resident)

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("unable to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV: %w", err)
	}

	// Skip header if present (assuming it is)
	for i, record := range records {
		if i == 0 && record[0] == "id" {
			continue
		}
		if len(record) < 4 {
			return nil, fmt.Errorf("invalid record at line %d: %v", i+1, record)
		}

		// Parse ID
		id, err := strconv.Atoi(record[0])
		if err != nil {
			return nil, fmt.Errorf("invalid ID at line %d: %w", i+1, err)
		}

		if _, exists := residents[id]; exists {
			fmt.Println(id)
		}

		residents[id] = &Resident{
			residentID:     id,
			firstname:      record[1],
			lastname:       record[2],
			rol:            parseRol(record[3]),
			matchedProgram: "",
		}
	}

	return residents, nil
}

// reads a CSV file into a map of Program
func ReadProgramsCSV(filename string) (map[string]*Program, error) {

	// map to store programs by ID
	programs := make(map[string]*Program)

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("unable to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV: %w", err)
	}

	// Skip header if present (assuming it is)
	for i, record := range records {
		if i == 0 && record[0] == "id" {
			continue
		}
		if len(record) < 4 {
			return nil, fmt.Errorf("invalid record at line %d: %v", i+1, record)
		}

		// Parse number of positions
		np, err := strconv.Atoi(record[2])
		if err != nil {
			return nil, fmt.Errorf("invalid number at line %d: %w", i+1, err)
		}

		programs[record[0]] = &Program{
			programID:  record[0],
			name:       record[1],
			nPositions: np,
			rol:        parseIntRol(record[3]),
		}

	}

	return programs, nil
}

// ------- SINGLE THREADED ----------- *Remove mutex line at top of code to use
/*
func offer(resId int, residents map[int]*Resident, programs map[string]*Program) {
	res := residents[resId]

	//If resident has exhausted programs on their list
	if res.rolIndex >= len(res.rol) {
		res.matchedProgram = ""
		return
	}

	// Move to next program on residents list
	progId := res.rol[res.rolIndex]
	res.rolIndex++

	evaluate(resId, progId, residents, programs)
}

func evaluate(resId int, progId string, residents map[int]*Resident, programs map[string]*Program) {

	res := residents[resId]
	prog := programs[progId]

	// check if prog ranked this resident
	rankedPos := -1
	for i, id := range prog.rol {
		if id == resId {
			rankedPos = i
			break
		}
	}

	// prog didnt rank this res, try next program
	if rankedPos == -1 {
		offer(resId, residents, programs)
		return
	}

	displacedId, wasDisplaced := prog.selectedResidents.push(res, prog)

	if wasDisplaced {
		// new resident accepted, removed res tries next program
		res.matchedProgram = progId
		residents[displacedId].matchedProgram = ""
		offer(displacedId, residents, programs)

	} else if res.matchedProgram != progId {
		// didnt add the resident
		offer(resId, residents, programs)
	}
	// if push returned false but did add the resident, matchedProgram already set
}
*/

// -------------- MULTITHREADED VERSION -------------------------

func offer(resId int, residents map[int]*Resident, programs map[string]*Program, wg *sync.WaitGroup) {
	defer wg.Done()

	res := residents[resId]

	//If resident has exhausted programs on their list
	mu.Lock()
	if res.rolIndex >= len(res.rol) {
		res.matchedProgram = ""
		mu.Unlock()
		return
	}

	// Move to next program on residents list
	progId := res.rol[res.rolIndex]
	res.rolIndex++
	mu.Unlock()

	evaluate(resId, progId, residents, programs, wg)
}

func evaluate(resId int, progId string, residents map[int]*Resident, programs map[string]*Program, wg *sync.WaitGroup) {

	res := residents[resId]
	prog := programs[progId]

	// check if prog ranked this resident
	rankedPos := -1
	for i, id := range prog.rol {
		if id == resId {
			rankedPos = i
			break
		}
	}

	// prog didnt rank this res, try next program
	if rankedPos == -1 {
		wg.Add(1)
		go offer(resId, residents, programs, wg)
		return
	}

	// lock before changing shared data
	mu.Lock()
	displacedId, wasDisplaced := prog.selectedResidents.push(res, prog)

	// new resident accepted, removed res tries next program
	if wasDisplaced {
		res.matchedProgram = progId
		residents[displacedId].matchedProgram = ""
		mu.Unlock()
		wg.Add(1)
		go offer(displacedId, residents, programs, wg)

	} else {
		// check inside lock
		alreadyMatched := res.matchedProgram == progId
		mu.Unlock()

		if !alreadyMatched {
			// didnt add resident
			wg.Add(1)
			go offer(resId, residents, programs, wg)
		}
	}
	// if push returned false but did add the resident, matchedProgram already set
}

// Example usage
func main() {

	// read residents
	residents, err := ReadResidentsCSV("residents4000.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	/*
		for _, p := range residents {
			fmt.Printf("ID: %d, Name: %s %s, Rol: %v\n", p.residentID, p.firstname, p.lastname, p.rol)
		}
	*/

	programs, err := ReadProgramsCSV("programs4000.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	/*
		for _, p := range programs {
			fmt.Printf("ID: %s, Name: %s, Number of pos: %d, Number of applicants: %d\n", p.programID, p.name, p.nPositions, len(p.rol))
		}

		fmt.Printf("\nNMD: %v", programs["NMD"])
	*/

	var wg sync.WaitGroup

	start := time.Now()

	for value := range residents {
		wg.Add(1)
		go offer(value, residents, programs, &wg)
	}

	wg.Wait()

	end := time.Now()

	// sort alphabetically
	var residentList []*Resident
	for _, r := range residents {
		residentList = append(residentList, r)
	}

	sort.Slice(residentList, func(i, j int) bool {
		if residentList[i].lastname == residentList[j].lastname {
			return residentList[i].firstname < residentList[j].firstname
		}
		return residentList[i].lastname < residentList[j].lastname
	})

	fmt.Println("lastname, firstname, residentID, programID, name")

	unmatchedCounter := 0
	for _, resident := range residentList {

		fmt.Printf("%s, ", resident.lastname)
		fmt.Printf("%s, ", resident.firstname)
		fmt.Printf("%d, ", resident.residentID)

		if resident.matchedProgram == "" {
			fmt.Printf("XXX, ")
			fmt.Printf("NOT_MATCHED")
			unmatchedCounter++
		} else {
			p := programs[resident.matchedProgram]
			fmt.Printf("%s, ", p.programID)
			fmt.Printf("%s", p.name)
		}

		fmt.Println()
	}

	positionsLeft := 0
	for i := range programs {
		program := programs[i]
		positionsLeft += program.nPositions - len(program.selectedResidents.theListHeap)
	}

	fmt.Printf("\nNumber of unmatched residents: %d\n", unmatchedCounter)
	fmt.Printf("Number of positions available: %d", positionsLeft)
	fmt.Printf("\nExecution time: %s", end.Sub(start))

}
