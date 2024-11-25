package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const version = "0.0.1"

var (
	versionFlag bool
	address     string
	password    string
	duration    string
	dataTypes   []string
	pipeline    int
	fieldsNum   int

	rootCmd = &cobra.Command{
		Use:   "redis-benchmark-go",
		Short: "redis-benchmark",
		Long:  "A redis benchmark tool",
		Run: func(cmd *cobra.Command, args []string) {
			if versionFlag {
				fmt.Println(fmt.Sprintf("redis-benchmark-go v%s", version))
				os.Exit(0)
			}
			run()
		},
	}
)

func run() {
	ctx := context.Background()
	var cancel context.CancelFunc

	if len(duration) > 0 {
		runDuration, err := time.ParseDuration(duration)

		if err != nil || runDuration < 1*time.Second {
			_, _ = fmt.Fprintf(os.Stderr, "invalid duration: %s\n", duration)
			os.Exit(1)
		}
		ctx, cancel = context.WithTimeout(ctx, runDuration)
		defer cancel()
	}

	dataGenerators := make([]*DataGenerator, 0, len(dataTypes))

	for _, t := range dataTypes {
		if len(t) == 0 {
			continue
		}
		generator := NewDataGenerator(Type(strings.ToLower(t)), fieldsNum, 10)
		dataGenerators = append(dataGenerators, generator)
	}

	dataCh := RunDataGenerators(ctx, dataGenerators...)

	sender := NewSender(3, address, password, pipeline, dataCh)
	sender.Run(ctx)
}

func main() {
	rootCmd.Root().CompletionOptions.DisableDefaultCmd = true
	rootCmd.Flags().BoolVarP(&versionFlag, "version", "v", false, "print the version info")
	rootCmd.Flags().StringVarP(&address, "address", "a", "", "the address of redis server")
	rootCmd.Flags().StringVarP(&password, "password", "p", "", "the password of redis server")
	rootCmd.Flags().StringVarP(&duration, "duration", "d", "", "the duration of running(unit: s, m, h), must > 1s")
	rootCmd.Flags().StringSliceVarP(&dataTypes, "types", "t", []string{"string"},
		"data type(use commas to separate multiple), support string, list, set, hash, zset")
	rootCmd.Flags().IntVar(&pipeline, "pipeline", 16, "the pipeline of redis client")
	rootCmd.Flags().IntVar(&fieldsNum, "fields", 8, "the fields number of hash, zset, set, list data")

	_ = rootCmd.MarkFlagRequired("duration")

	for i := 0; i < len(dataTypes); i++ {
		if !IsSupportedType(strings.ToLower(dataTypes[i])) {
			_, _ = fmt.Fprintf(os.Stderr, "unsupported data type: %s\n", dataTypes[i])
			os.Exit(1)
		}
		dataTypes[i] = strings.ToLower(dataTypes[i])
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}