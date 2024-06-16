package cmd

import "github.com/spf13/cobra"

type Command struct {
	// The underlying cobra command.
	command *cobra.Command
	// A configuration function that can be used to configure Litebase before running the command.
	configFunc func(cmd *cobra.Command)
	// A flags function that can be used to add flags to the command.
	flagsFunc func(cmd *cobra.Command)
	// Use is the one-line usage message.
	Use string
	// Short is the short description shown in the 'help' output.
	Short string
}

func NewCommand(use, short string) *Command {
	return &Command{
		command: &cobra.Command{
			Use:   use,
			Short: short,
		},
	}
}

func (c *Command) Build() *cobra.Command {
	if c.flagsFunc != nil {
		c.flagsFunc(c.command)
	}

	return c.command
}

func (c *Command) WithConfig(config func(cmd *cobra.Command)) *Command {
	c.configFunc = config

	return c
}

func (c *Command) WithFlags(flags func(cmd *cobra.Command)) *Command {
	c.flagsFunc = flags

	return c
}

func (c *Command) WithRun(run func(cmd *cobra.Command, args []string)) *Command {
	c.command.Run = func(cmd *cobra.Command, args []string) {
		if c.configFunc != nil {
			c.configFunc(cmd)
		}

		run(cmd, args)
	}

	return c
}
