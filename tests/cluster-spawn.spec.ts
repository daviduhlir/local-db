import { expect } from 'chai'
import { spawn } from 'child_process'
import * as path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

describe('Cluster Tests (via spawn)', () => {
  it('should handle concurrent inserts from multiple workers', async function() {
    this.timeout(20000)

    const appPath = path.join(__dirname, 'cluster-test-app.js')

    return new Promise<void>((resolve, reject) => {
      const proc = spawn('node', [appPath], {
        stdio: 'pipe',
        env: { ...process.env },
      })

      let output = ''
      let errorOutput = ''

      proc.stdout.on('data', data => {
        const text = data.toString()
        output += text
        console.log(text.trim())
      })

      proc.stderr.on('data', data => {
        const text = data.toString()
        errorOutput += text
        console.error(text.trim())
      })

      proc.on('exit', code => {
        if (code === 0) {
          expect(output).to.include('All tests passed!')
          resolve()
        } else {
          reject(
            new Error(
              `Cluster test failed with exit code ${code}\n\nOutput:\n${output}\n\nErrors:\n${errorOutput}`
            )
          )
        }
      })

      proc.on('error', err => {
        reject(new Error(`Failed to start process: ${err.message}`))
      })
    })
  })
})