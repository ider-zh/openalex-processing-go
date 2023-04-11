/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"openalex/internal/logic/googledistance"

	"github.com/spf13/cobra"
)

// googleDistanceCmd represents the googleDistance command
var googleDistanceCmd = &cobra.Command{
	Use:   "googleDistance",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		googledistance.Main()
		fmt.Println("googleDistance over")
	},
}

func init() {
	rootCmd.AddCommand(googleDistanceCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// googleDistanceCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// googleDistanceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
