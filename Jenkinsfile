/**
 * Purpose: bazel-buildfarm Jenkinsfile
 * Description: Reads repo-metadata.yaml to determine project directory structure,
 *   calculates which projects were modified, and runs the pipeline steps
 *   defined in each modified project's project-metadata.yaml file.
 *
 *   Expected repo-metadata.yaml structure:
 *      name: ${name_of_repo}
 *      maintainer: ${email_address_of_maintainer}
 *      projects:
 *      - name: core:
 *        path: "core"
 *
 *   Expected project-metadata.yaml structure:
 *      name: ${name_of_project}
 *      maintainer: ${email_address_of_maintainer}
 *      pipeline:
 *      - name: ${step_name}
 *        command: ${bash_command_with_arguments}
 *        with_credentials: ${name_of_jenkins_credential} // USERNAME and PASSWORD env_vars will be set
 *        conditionally: ${branch_or_pr}
 */

@Library('groovy_lib@main')

import org.lat.offboard.PipelineBuilder
import org.lat.utils.ActionHandler

PipelineBuilder pipelineBuilder = new PipelineBuilder(this)
pipelineBuilder.branchFailureSlackChannel = '#buildfarm-github-repo'
pipelineBuilder.runBuild()
