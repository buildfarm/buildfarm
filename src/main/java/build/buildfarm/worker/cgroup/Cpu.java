// Copyright 2020 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.cgroup;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.logging.Level.SEVERE;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.StringTokenizer;
import lombok.extern.java.Log;

@Log
public class Cpu extends Controller {
  private static final int CPU_GRANULARITY = 100_000; // 100 milliseconds (mS)

  Cpu(Group group) {
    super(group);
  }

  @Override
  public String getControllerName() {
    return "cpu";
  }

  @Override
  public Map<String, Long> sample() {
    ImmutableMap.Builder<String, Long> sample = ImmutableMap.builder();
    Path statPath = getPath().resolve("cpu.stat");
    Path pressurePath = getPath().resolve("cpu.pressure");

    char[] data = new char[1024];
    int len;
    try (Reader in = new InputStreamReader(Files.newInputStream(statPath))) {
      len = in.read(data);
      if (len == data.length) {
        throw new RuntimeException("cpu.stat data too long");
      }
      // codepoint?
      String name = null;
      for (StringTokenizer st = new StringTokenizer(new String(data)); st.hasMoreTokens(); ) {
        String token = st.nextToken();
        if (name == null) {
          name = token;
        } else {
          sample.put(getControllerName() + "." + name, Long.parseLong(token));
          name = null;
        }
      }
    } catch (IOException e) {
      log.log(SEVERE, "error reading " + statPath, e);
    }

    try (Reader in = new InputStreamReader(Files.newInputStream(pressurePath))) {
      len = in.read(data);
      if (len == data.length) {
        throw new RuntimeException("cpu.pressure data too long");
      }
      // codepoint?
      String name = null;
      // some avg10=0.00 avg60=0.00 avg300=0.00 total=0
      boolean hundredths = false;
      long value_hths = 0;
      boolean first = true;
      for (StringTokenizer st =
              new StringTokenizer(new String(data), " \t\n\r\f=.", /* returnDelims= */ true);
          st.hasMoreTokens(); ) {
        String token = st.nextToken();
        if (first) {
          first = false;
        } else if (token.equals("\n") || token.equals(" ")) {
          if (name != null) {
            sample.put(getControllerName() + ".pressure." + name, value_hths);
            name = null;
          }
          // only concern ourselves with some
          if (token.equals("\n")) {
            break;
          }
        } else if (!token.equals("=") && !token.equals("some")) {
          if (name == null) {
            name = token;
          } else if (token.equals(".")) {
            hundredths = true;
          } else {
            long value = Long.parseLong(token);
            if (hundredths) {
              value_hths += value;
              hundredths = false;
            } else {
              value_hths = value * 100;
            }
          }
        }
      }
    } catch (IOException e) {
      log.log(SEVERE, "error reading " + pressurePath, e);
    }

    return sample.build();
  }

  /**
   * Represents how much CPU shares are allocated.
   *
   * @see <a
   *     href="https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#weights">Cgroups
   *     v2: Weights</a>
   * @param shares how many vCPUs to give. a value of one means one CPU core.
   * @throws IOException
   */
  public void setShares(int shares) throws IOException {
    if (shares > 0) {
      open();
      // If you really have a 1000-core machine, please patch this =)
      checkArgument(shares < 1000, "shares must be less than 1000");
      // cpu.weight in the range [1,10_000]
      // the default is 100, so we want to set something every time.
      writeInt("cpu.weight", shares);
    }
  }

  /**
   * Set the maximum number of cores.
   *
   * @param cpuCores whole cores. 1 == 1 CPU core.
   */
  public void setMaxCpu(int cpuCores) throws IOException {
    setCpu(cpuCores * CPU_GRANULARITY);
  }

  @Override
  public void setCpu(int cpu_us) throws IOException {
    open();
    writeIntPair("cpu.max", cpu_us, CPU_GRANULARITY);
  }
}
