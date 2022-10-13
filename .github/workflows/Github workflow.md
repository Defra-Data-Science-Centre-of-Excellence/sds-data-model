# Git branching strategy recommendation


## What should we be doing?

In a nut shell, the thing you're working on should be defined in a GitHub issue, you should be working on it in a dedicated branch, and your pull request to re-integrate that branch should directly reference the motivating issue.



## Why should we be doing this?
Focusing branches on one issue should make it easier to re-integrate into the main branch as, in theory, your changes will be confined to a smaller section of the code based and less likely to conflict with changes made by others.
If you're using a GitHub (or Jira) project board (with automation), this approach will keep it in sync with your progress


## Okay, you've convinced us, how do we do this?
When you want to work on an issue that's been assigned to you, create a branch from main that references it. For example, if I wanted to work on Issue 20, I would create a branch that referenced by GitHub username and the issue, i.e. EFT-Defra/issue20. You don't have to do this but it makes it clear who's working on what (and it's what the VSCode GitHub extension does).
Write some awesome code.
When you're ready to re-integrate your code into main, trigger a pull request that references the issue either by using keywords, like "Closes #20", in the pull request description or by manually linking them .
If GitHub project board automation is enabled, merging the pull request will close the issue.


## Anything else to consider?
Pull requests are a useful way of getting feedback on your code. If you want people to start looking at and or commenting on your code before you're ready to re-integrate, you can use a draft pull request.
If you use the GitHub CLI to create a pull request, it'll create a description from your commit messages, which can be nice, especially as it encourages you to write commit messages in a way you'll be able to understand later. There's quite a lot of writing out there about how to write them (and even entire standards ,such as Conventional Commits), I like this summary from Chelsea Troy.

## To add
- if you have comments on pull request, what do you do? do you re-pull?