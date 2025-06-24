package cmd

import "github.com/spf13/cobra"

type Command struct {
	// The underlying cobra command.
	command *cobra.Command
	// A configuration function that can be used to configure Litebase before running the command.
	configFunc func(cmd *cobra.Command)
	// A configuration function that returns an error if the configuration is invalid.
	configFuncE func(cmd *cobra.Command) error
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

func (c *Command) WithRunE(run func(cmd *cobra.Command, args []string) error) *Command {
	c.command.RunE = func(cmd *cobra.Command, args []string) error {
		if c.configFuncE != nil {
			if err := c.configFuncE(cmd); err != nil {
				return err
			}
		}

		return run(cmd, args)
	}

	return c
}
