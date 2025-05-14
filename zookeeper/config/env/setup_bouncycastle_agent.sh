#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -u  # Treat unset variables as an error

# Variables
AGENT_NAME="BouncyCastleAgent"
AGENT_CLASS="BouncyCastleAgent"
AGENT_JAVA_FILE="/tmp/${AGENT_CLASS}.java"
AGENT_JAR="/apache-zookeeper-3.8.4-bin/lib/${AGENT_CLASS}.jar"
FAT_AGENT_JAR="/apache-zookeeper-3.8.4-bin/lib/${AGENT_CLASS}-fat.jar"
MANIFEST_FILE="/tmp/MANIFEST.MF"
BC_PROV_JAR="/apache-zookeeper-3.8.4-bin/lib/bcprov-jdk18on-1.79.jar"
TEMP_DIR="/tmp/fat_jar_temp"
ZK_ENV_SH="/apache-zookeeper-3.8.4-bin/bin/zkEnv.sh"

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Ensure necessary commands are available
for cmd in javac jar grep sed unzip; do
    if ! command_exists "$cmd"; then
        echo "Error: Required command '$cmd' not found. Please install it before running this script."
        exit 1
    fi
done

# Step 1: Create the Java Agent source code
echo "Creating Java Agent source code at ${AGENT_JAVA_FILE}..."
cat > "${AGENT_JAVA_FILE}" <<EOF
import java.security.Security;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import java.lang.instrument.Instrumentation;

public class ${AGENT_CLASS} {
    public static void premain(String agentArgs, Instrumentation inst) {
        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
            System.out.println("BouncyCastle Provider Registered Successfully.");
        } else {
            System.out.println("BouncyCastle Provider Already Registered.");
        }
    }
}
EOF

# Step 2: Compile the Java Agent
echo "Compiling Java Agent..."
javac -cp "${BC_PROV_JAR}" "${AGENT_JAVA_FILE}"

# Step 3: Create the Manifest file
echo "Creating manifest file at ${MANIFEST_FILE}..."
cat > "${MANIFEST_FILE}" <<EOF
Premain-Class: ${AGENT_CLASS}
EOF

# Step 4: Package the Agent into a JAR
echo "Packaging Java Agent into JAR at ${AGENT_JAR}..."
jar cmf "${MANIFEST_FILE}" "${AGENT_JAR}" -C /tmp "${AGENT_CLASS}.class"

# Step 5: Create a Fat (Uber) JAR by bundling BouncyCastleProvider
echo "Creating a temporary directory at ${TEMP_DIR} for building the Fat JAR..."
mkdir -p "${TEMP_DIR}"

# Ensure the temporary directory is clean
echo "Cleaning temporary directory ${TEMP_DIR}..."
rm -rf "${TEMP_DIR:?}/"*

echo "Extracting ${AGENT_JAR} into ${TEMP_DIR}..."
unzip -o -q "${AGENT_JAR}" -d "${TEMP_DIR}"

echo "Extracting ${BC_PROV_JAR} into ${TEMP_DIR}, excluding META-INF/* to prevent overwrite prompts..."
unzip -o -q "${BC_PROV_JAR}" -d "${TEMP_DIR}" -x "META-INF/*"

echo "Repackaging the Fat JAR at ${FAT_AGENT_JAR} with all dependencies..."
# Recreate the Fat JAR with the combined classes and the original manifest
jar cmf "${MANIFEST_FILE}" "${FAT_AGENT_JAR}" -C "${TEMP_DIR}" .

# Step 6: Verify the Fat JAR contents
echo "Verifying the contents of the Fat Agent JAR..."
if jar tf "${FAT_AGENT_JAR}" | grep -q "${AGENT_CLASS}.class" && \
   jar tf "${FAT_AGENT_JAR}" | grep -q "org/bouncycastle/jce/provider/BouncyCastleProvider.class"; then
    echo "Verification successful: Fat Agent JAR contains both ${AGENT_CLASS}.class and BouncyCastleProvider classes."
else
    echo "Error: Fat Agent JAR does not contain all necessary classes."
    exit 1
fi

# Clean up temporary files
echo "Cleaning up temporary files..."
rm -rf "${TEMP_DIR}" "${AGENT_JAVA_FILE}" "${MANIFEST_FILE}" "/tmp/${AGENT_CLASS}.class"

# Step 7: Modify zkEnv.sh to include the Fat Java Agent
echo "Modifying ${ZK_ENV_SH} to include the Fat Java Agent..."

# Backup the original zkEnv.sh
echo "Creating backup of ${ZK_ENV_SH} at ${ZK_ENV_SH}.bak..."
sudo cp "${ZK_ENV_SH}" "${ZK_ENV_SH}.bak"

# Check if the Java Agent is already present to avoid duplication
if sudo grep -q "${FAT_AGENT_JAR}" "${ZK_ENV_SH}"; then
    echo "Fat Java Agent already present in ${ZK_ENV_SH}. Skipping modification."
else
    # Insert the Java Agent into JVMFLAGS
    # This assumes JVMFLAGS is defined and possibly spans multiple lines
    # We'll append the javaagent parameter at the end of JVMFLAGS

    echo "Appending -javaagent to JVMFLAGS in ${ZK_ENV_SH}..."
    sudo sed -i "/JVMFLAGS=/ s|\"$| \\\n    -javaagent:${FAT_AGENT_JAR} \\\n    |" "${ZK_ENV_SH}"

    echo "Fat Java Agent added to JVMFLAGS in ${ZK_ENV_SH}."
fi

# Step 8: Verify the Agent Registration
echo "Verifying BouncyCastle provider registration..."

# Create a temporary Java program to list security providers
VERIFICATION_JAVA_FILE="/tmp/VerifyBouncyCastle.java"
VERIFICATION_CLASS="VerifyBouncyCastle"

echo "Creating verification Java program at ${VERIFICATION_JAVA_FILE}..."
sudo cat > "${VERIFICATION_JAVA_FILE}" <<EOF
import java.security.Security;

public class ${VERIFICATION_CLASS} {
    public static void main(String[] args) {
        for (java.security.Provider provider : Security.getProviders()) {
            System.out.println(provider.getName());
        }
    }
}
EOF

# Compile the verification program with the Fat Agent in classpath
echo "Compiling verification program..."
sudo javac -cp "${FAT_AGENT_JAR}:${BC_PROV_JAR}" "${VERIFICATION_JAVA_FILE}"

# Run the verification program with the Java Agent
echo "Running verification program..."
VERIFICATION_OUTPUT=$(java -javaagent:"${FAT_AGENT_JAR}" -cp "${FAT_AGENT_JAR}:${BC_PROV_JAR}:/tmp" "${VERIFICATION_CLASS}" 2>/dev/null)

# Clean up the verification Java files
echo "Cleaning up verification Java files..."
rm -f "${VERIFICATION_JAVA_FILE}" "/tmp/${VERIFICATION_CLASS}.class"

# Check if BouncyCastle provider is listed
if echo "${VERIFICATION_OUTPUT}" | grep -q "BC"; then
    echo "BouncyCastle Provider is successfully registered."
else
    echo "Error: BouncyCastle Provider registration failed."
    exit 1
fi

echo "BouncyCastle Agent setup completed successfully."
