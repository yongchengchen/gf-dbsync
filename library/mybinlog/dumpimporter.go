package mybinlog

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/gogf/gf/v2/frame/g"
)

func (d *MyDumper) ImportSql(sqlFilePath string, toServer string, toDB string) {
	// Read the SQL file
	file, err := os.Open(sqlFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var modifiedLines []string
	var masterLogFile, masterLogPos string
	lineCount := 0
	// Iterate over each line of the SQL file
	for scanner.Scan() {
		line := scanner.Text()

		// Check if the line contains the "CHANGE MASTER TO" statement
		if lineCount < 50 && strings.Contains(line, "CHANGE MASTER TO") {
			// Extract the MASTER_LOG_FILE and MASTER_LOG_POS values
			parts := strings.Split(line, "MASTER_LOG_FILE='")
			if len(parts) < 2 {
				continue
			}

			masterLogFileParts := strings.Split(parts[1], "',")
			if len(masterLogFileParts) < 2 {
				continue
			}

			masterLogFile = strings.TrimSpace(masterLogFileParts[0])

			posParts := strings.Split(parts[1], "MASTER_LOG_POS=")
			if len(posParts) < 2 {
				continue
			}

			masterLogPosParts := strings.Split(posParts[1], ";")
			if len(masterLogPosParts) < 1 {
				continue
			}

			masterLogPos = strings.TrimSpace(masterLogPosParts[0])

			// Skip the "CHANGE MASTER TO" line by not appending it to the modifiedLines slice
			continue
		}

		// Append the line to the modifiedLines slice
		modifiedLines = append(modifiedLines, line)
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Create a new SQL file with the modified content
	outputFilePath := sqlFilePath + ".new"
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()

	// Write the modified lines to the new SQL file
	for _, line := range modifiedLines {
		fmt.Fprintln(outputFile, line)
	}

	// Print the extracted MASTER_LOG_FILE and MASTER_LOG_POS values
	fmt.Println("MASTER_LOG_FILE:", masterLogFile)
	fmt.Println("MASTER_LOG_POS:", masterLogPos)

	// Execute any further operations using the extracted values if needed
	// For example, you can use them in subsequent MySQL operations

	// Execute the MySQL command to perform further actions with the extracted values
	// cmd := exec.Command("mysql", "-u", "yourusername", "-pYourPassword", "-e", fmt.Sprintf(`USE Test1; CHANGE MASTER TO MASTER_LOG_FILE='%s', MASTER_LOG_POS=%s;`, masterLogFile, masterLogPos))

	c := g.DB(toServer).GetConfig()
	// args := make([]string, 0, 16)
	// args = append(args, fmt.Sprintf(" -u%s", c.User))
	// args = append(args, fmt.Sprintf(" -p%s", c.Pass))
	// args = append(args, fmt.Sprintf(" -h %s", c.Host))
	// args = append(args, fmt.Sprintf(" -P %s", c.Port))
	// args = append(args, fmt.Sprintf(" < %s", outputFilePath))
	// cmd := exec.Command(d.ExecutionPath, args...)

	cmd := exec.Command("mysql",
		"-u", c.User,
		"-p"+c.Pass,
		"-h", c.Host,
		"-P", c.Port,
		"-D", toDB,
		"-e", "source "+outputFilePath,
	)
	err = cmd.Run()
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	os.Remove(outputFilePath)
}
