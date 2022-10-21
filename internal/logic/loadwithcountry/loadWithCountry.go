package loadwithcountry

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"openalex/internal/loadfile"
	"openalex/internal/mode"
	"os"
	"strconv"
	"strings"
)

// dump reference to mongo, add paper country

func Main() {
	// country iso name map
	COUNTRY_MAP := make(map[string]string)
	filePath := "/home/ni/data/mag2020/sx_country_target_zh.txt"
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		strs := strings.Split(scanner.Text(), "\t")
		if len(strs) == 4 {

			codeName := strings.TrimSpace(strs[0])
			countryName1 := strings.TrimSpace(strs[1])
			countryName2 := strings.TrimSpace(strs[2])
			COUNTRY_MAP[countryName1] = codeName
			COUNTRY_MAP[countryName2] = codeName

		} else {
			fmt.Printf("len(strs): %v %v\n", len(strs), strs)
		}
	}
	for k, v := range COUNTRY_MAP {
		fmt.Printf("ORG_COUNTRY_MAP: %v  :  %v\n", k, v)
		break
	}
	fmt.Printf("ORG_COUNTRY_MAP: %v\n", len(COUNTRY_MAP))

	// load autor fountry map
	ORG_COUNTRY_MAP := make(map[string]string)
	filePath = "/home/ni/data/mag2020/noOrgId_country_openAlex_zh.txt"
	file, err = os.Open(filePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		strs := strings.Split(scanner.Text(), "\t")
		if len(strs) > 1 {
			var countrysList []string
			err = json.Unmarshal([]byte(strs[len(strs)-1]), &countrysList)
			if err != nil {
				log.Fatalln(err)
			}
			if len(countrysList) == 1 && countrysList[0] != "" {
				if isoNmae, ok := COUNTRY_MAP[countrysList[0]]; ok {
					orgStr := strings.TrimSpace(strings.Join(strs[:len(strs)-1], " "))
					ORG_COUNTRY_MAP[orgStr] = isoNmae
				} else {
					fmt.Printf("countrysList[0]: no iso name [%v]\n", countrysList)
				}
			}
		} else {
			fmt.Printf("len(strs): %v %v\n", len(strs), strs)
		}
	}
	for k, v := range ORG_COUNTRY_MAP {
		fmt.Printf("ORG_COUNTRY_MAP: %v  :  %v\n", k, v)
		break
	}
	fmt.Printf("ORG_COUNTRY_MAP: %v\n", len(ORG_COUNTRY_MAP))

	// load works

	chanOut := make(chan mode.WorkMongo, 1000)

	var handlePipeLine = func(s mode.WorkSource) {
		countryCode := ""
	FindCountry:
		for _, obj := range s.AuthorShips {
			for _, ins := range obj.Institutions {
				if ins.CountryCode != "" {
					countryCode = ins.CountryCode
					break FindCountry
				}
				var ok bool
				if countryCode, ok = ORG_COUNTRY_MAP[ins.Name]; ok {
					break FindCountry
				}
			}
		}
		ID, err := strconv.ParseInt(s.ID[1:], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		dataOut := mode.WorkMongo{
			ID:   ID,
			Year: s.Year,
		}
		if countryCode != "" {
			dataOut.Country = countryCode
		}

		idOuts := []int64{}
		for _, idString := range s.Ref {
			IDR, err := strconv.ParseInt(idString[1:], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			idOuts = append(idOuts, IDR)
		}
		if len(idOuts) > 0 {
			dataOut.Out = idOuts
		}

		chanOut <- dataOut
	}

	// handle ret data
	go func() {

	}()

	ffc := []func(mode.WorkSource){handlePipeLine}
	for row := range loadfile.ExtractingWork(ffc) {
		log.Info(row.ID)
	}

}
