#!/bin/bash

echo "$(date '+%Y-%m-%d %H:%M:%S') - Generate-Keystore-Script Has Started..."

# Function to handle exit status and log with a timestamp
handle_exit() {
  if [ $? -ne 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: something went wrong. Exiting."
    exit 1
  fi
}

# Function to generate a very strong, random PEM passphrase with a diverse character set
generate_strong_passphrase() {
  echo "$(openssl rand -base64 66 | tr -dc 'A-Za-z0-9' | head -c 88)"
}

# Define the location to store the passphrase
PEM_PASSPHRASE_FILE="/conf/ssl/pem_passphrase.txt"

# Ensure the SSL directory exists
SSL_DIR="/conf/ssl/"
mkdir -p "$SSL_DIR"
handle_exit

# Check if the passphrase file exists
if [ -f "$PEM_PASSPHRASE_FILE" ]; then
  PEM_PASSPHRASE=$(cat "$PEM_PASSPHRASE_FILE")
  echo "Using existing PEM passphrase from file."
else
  PEM_PASSPHRASE=$(generate_strong_passphrase)
  echo "$PEM_PASSPHRASE" > "$PEM_PASSPHRASE_FILE"
  chmod 600 "$PEM_PASSPHRASE_FILE"  # Secure the passphrase file
  echo "Generated and saved new PEM passphrase."
fi

echo "PEM passphrase: $PEM_PASSPHRASE"
handle_exit
sleep 5

# Function to install CA certificate into the system's trusted CA store
install_ca_certificate() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Installing CA certificate into system's trusted CA store..."

  CA_CERT_PATH="/conf/ssl/ca-cert.crt"

  # Determine the OS and set the appropriate CA store path
  if [ -d "/usr/local/share/ca-certificates/" ]; then
    # Debian-based systems
    SYSTEM_CA_STORE="/usr/local/share/ca-certificates/myrootca.crt"
    sudo cp "$CA_CERT_PATH" "$SYSTEM_CA_STORE"
    handle_exit
    sudo chmod 644 "$SYSTEM_CA_STORE"
    handle_exit
    sudo update-ca-certificates
    handle_exit
  elif [ -d "/etc/pki/ca-trust/source/anchors/" ]; then
    # Red Hat-based systems
    SYSTEM_CA_STORE="/etc/pki/ca-trust/source/anchors/myrootca.crt"
    cp "$CA_CERT_PATH" "$SYSTEM_CA_STORE"
    handle_exit
    sudo chmod 644 "$SYSTEM_CA_STORE"
    handle_exit
    sudo update-ca-trust
    handle_exit
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Unsupported OS for automatic CA installation. Please install the CA certificate manually."
    exit 1
  fi

  echo "$(date '+%Y-%m-%d %H:%M:%S') - CA certificate installed successfully into system's trusted CA store."
}

# Function to validate the CSR
validate_csr() {
  CSR_FILE=$1
  if openssl req -in "$CSR_FILE" -noout -text >/dev/null 2>&1; then
    echo "Valid CSR: $CSR_FILE"
    return 0
  else
    echo "Invalid CSR: $CSR_FILE"
    return 1
  fi
}

# Define a mapping of node names to their IPv4
declare -A NODE_IPS=(
  [zookeeper-quorum-server1]="172.16.10.1"
  [zookeeper-quorum-server2]="172.16.10.2"
  [zookeeper-quorum-server3]="172.16.10.3"
  [zookeeper-quorum-learner4]="172.16.10.4"
  [zookeeper-quorum-learner5]="172.16.10.5"
  [zoonavigator]="172.16.10.8"  
)

# Define Zookeeper nodes
ALL_NODES="zookeeper-quorum-server1 zookeeper-quorum-server2 zookeeper-quorum-server3 zookeeper-quorum-learner4 zookeeper-quorum-learner5 zoonavigator"

# Define admin separately as it doesn't have an associated IP
ADMIN_NAME="admin"

# Paths and passwords
CA_CERT_PATH="/conf/ssl/ca-cert.crt"
CA_KEY_PATH="/conf/ssl/ca-key.key"
TRUSTSTORE_PASSWORD="NeloNELO123456789"
KEYSTORE_PASSWORD="NeloNELO123456789"

# Define a mapping of node names to their roles for dynamic DNAME generation
declare -A NODE_ROLES=(
  [zookeeper-quorum-server1]="zookeeper"
  [zookeeper-quorum-server2]="zookeeper"
  [zookeeper-quorum-server3]="zookeeper"
  [zookeeper-quorum-learner4]="zookeeper"
  [zookeeper-quorum-learner5]="zookeeper"
  [zoonavigator]="zoonavigator"
)

# Function to generate DNAME based on node name
generate_dname() {
  local node=$1
  local role=${NODE_ROLES[$node]}
  
  if [ -n "$role" ]; then
    echo "CN=${node}, OU=${role}, O=${role}, L=Miami, ST=FL, C=US"
  else
    echo "CN=${node}, OU=${role}, O=${role}, L=Miami, ST=FL, C=US"
  fi
}

# Function to verify that the alias exists in the keystore, and create it if it doesn't
verify_and_create_alias() {
  ALIAS=$1
  STORE_PATH=$2
  STORE_PASSWORD=$3
  DNAME=$4 # Distinguished Name
  STORE_TYPE=$5 # keystore or truststore
  NODE_IP=$6 # Optional IP address

  DELAY=5 

  if keytool -list -alias "$ALIAS" -keystore "$STORE_PATH" -storepass "$STORE_PASSWORD" -storetype PKCS12 >/dev/null 2>&1; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Alias $ALIAS already exists in the $STORE_TYPE. Continuing..."
    return 0
  else
    if [ "$STORE_TYPE" = "keystore" ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Alias $ALIAS not found in keystore. Creating the alias."

      # Only include IP in SAN if NODE_IP is provided
      if [ -n "$NODE_IP" ]; then
        SAN_EXT="san=dns:$ALIAS,ip:$NODE_IP"
      else
        SAN_EXT="san=dns:$ALIAS"
      fi

      # Create the alias by generating a key pair using EC with secp384r1 curve and including SANs
      keytool -genkeypair -alias "$ALIAS" -keyalg EC -groupname secp384r1 -sigalg SHA384withECDSA -validity 3650 \
        -keystore "$STORE_PATH" -storepass "$STORE_PASSWORD" -dname "$DNAME" -keypass "$STORE_PASSWORD" \
        -noprompt \
        -ext "$SAN_EXT" -ext "keyUsage=digitalSignature,keyEncipherment" \
        -ext "extendedKeyUsage=serverAuth,clientAuth"
      handle_exit
      sleep 5

      # Verify the alias was successfully created
      if keytool -list -alias "$ALIAS" -keystore "$STORE_PATH" -storepass "$STORE_PASSWORD" -storetype PKCS12 >/dev/null 2>&1; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Alias $ALIAS successfully created in the keystore."
        return 0
      else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Alias $ALIAS could not be created in the keystore. Retrying in $DELAY seconds..."
        sleep $DELAY
        # Optionally, implement retry logic here
        exit 1
      fi
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - No need to create alias in truststore."
      return 0
    fi
  fi
}

# Phase 1: Certificate Generation
generate_certificates() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting Phase 1: Certificate Generation"

  # Step 1: Ensure the CA's private key and self-signed certificate are created and verified
  if [ ! -f "$CA_CERT_PATH" ] || [ ! -f "$CA_KEY_PATH" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Creating CA's private key and self-signed certificate..."

    # Generate EC private key with secp384r1 curve
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:secp384r1 -out "$CA_KEY_PATH" -aes256 -pass pass:"$PEM_PASSPHRASE"
    handle_exit
    sleep 5

    # Generate self-signed certificate using EC key
    openssl req -new -x509 -key "$CA_KEY_PATH" -out "$CA_CERT_PATH" -days 3650 -subj "/CN=MyRootCA" \
      -passin pass:"$PEM_PASSPHRASE" -sha256
    handle_exit
    sleep 5

    # Verify the creation by attempting to extract the public key
    if openssl pkey -in "$CA_KEY_PATH" -passin pass:"$PEM_PASSPHRASE" -pubout >/dev/null 2>&1 && \
       openssl x509 -in "$CA_CERT_PATH" -text -noout >/dev/null 2>&1; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Successfully created and verified CA's private key and self-signed certificate."
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Failed to create CA's private key and/or self-signed certificate."
      exit 1
    fi

    # Install the CA certificate into the system's trusted CA store
    install_ca_certificate
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - CA's private key and self-signed certificate already exist."
  fi

  # Iterate over each node in ALL_NODES to generate keystores and cert chains
  for NODE in $ALL_NODES; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating keystore and certificates for $NODE..."

    # Define server and client aliases
    SERVER_ALIAS="$NODE"
    CLIENT_ALIAS="${NODE}-client"

    # Generate dynamic DNAME based on node's role
    DNAME=$(generate_dname "$NODE")

    # Define paths for server and client certificates
    SERVER_KEYSTORE_PATH="/conf/ssl/${NODE}.keystore.p12"
    SERVER_CSR_PATH="/conf/ssl/${NODE}.csr"
    SERVER_SIGNED_CERT_PATH="/conf/ssl/${NODE}-signed.cert"
    SERVER_CERT_CHAIN_PATH="/conf/ssl/${NODE}-cert-chain.pem"

    CLIENT_KEYSTORE_PATH="/conf/ssl/${CLIENT_ALIAS}.keystore.p12"
    CLIENT_CSR_PATH="/conf/ssl/${CLIENT_ALIAS}.csr"
    CLIENT_SIGNED_CERT_PATH="/conf/ssl/${CLIENT_ALIAS}-signed.cert"
    CLIENT_CERT_CHAIN_PATH="/conf/ssl/${CLIENT_ALIAS}-cert-chain.pem"

    TRUSTSTORE_PATH="/conf/ssl/${NODE}.truststore.p12"

    # Extract IP addresses for the current node
    IPV4="${NODE_IPS[$NODE]}"

    # Validate that IPV4 is not null or empty
    if [ -z "$IPV4" ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: IP address for node $NODE is not set. Exiting."
      exit 1
    fi

    # Step 2: Verify that the CA private key is valid by attempting to extract the public key
    openssl pkey -in "$CA_KEY_PATH" -passin pass:"$PEM_PASSPHRASE" -pubout >/dev/null 2>&1
    handle_exit
    sleep 5

    ############################
    # Generate Server Certificate
    ############################
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating server keystore and certificates for $NODE..."

    # Step 3: Verify and create server alias in keystore
    verify_and_create_alias "$SERVER_ALIAS" "$SERVER_KEYSTORE_PATH" "$KEYSTORE_PASSWORD" "$DNAME" "keystore" "$IPV4"
    handle_exit

    # Step 4: Create the server CSR if not already created or invalid, including SANs
    if [ ! -f "$SERVER_CSR_PATH" ] || ! validate_csr "$SERVER_CSR_PATH"; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating CSR for server alias $SERVER_ALIAS..."
      # Updated SAN syntax to lowercase 'san'
      keytool -certreq -alias "$SERVER_ALIAS" -file "$SERVER_CSR_PATH" -keystore "$SERVER_KEYSTORE_PATH" -storepass "$KEYSTORE_PASSWORD" \
        -storetype PKCS12 -dname "$DNAME" -noprompt -v \
        -ext "san=dns:$SERVER_ALIAS,ip:$IPV4" -rfc
      handle_exit
      sleep 5

      # Verify CSR format
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Verifying the server CSR format..."
      if validate_csr "$SERVER_CSR_PATH"; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Server CSR is valid."
      else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Server CSR is invalid after regeneration."
        exit 1
      fi
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Server CSR already exists and is valid at $SERVER_CSR_PATH. Proceeding with signing."
    fi

    # Step 5: Generate the signed server certificate if it does not exist
    if [ ! -f "$SERVER_SIGNED_CERT_PATH" ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Signing the server CSR to create the signed server certificate..."

      EXTFILE=$(mktemp)  # Create a temporary file for the extension

      # Construct the SAN entries for server
      SAN_ENTRIES="[alt_names]
DNS.1 = ${SERVER_ALIAS}
IP.1 = ${IPV4}"

      cat <<EOF > "$EXTFILE"
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]
commonName = ${SERVER_ALIAS}

[v3_req]
subjectAltName = @alt_names
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth

$SAN_ENTRIES
EOF
      sleep 5

      # Sign the CSR using EC key with SHA384
      openssl x509 -req -in "$SERVER_CSR_PATH" -CA "$CA_CERT_PATH" -CAkey "$CA_KEY_PATH" -CAcreateserial \
        -out "$SERVER_SIGNED_CERT_PATH" -days 3650 -passin pass:"$PEM_PASSPHRASE" \
        -extensions v3_req -extfile "$EXTFILE" -sha384
      handle_exit
      sleep 5

      # Clean up the temporary extension file after use
      rm -f "$EXTFILE"
      handle_exit
      sleep 5

      echo "$(date '+%Y-%m-%d %H:%M:%S') - Signed server certificate created at $SERVER_SIGNED_CERT_PATH"
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Signed server certificate already exists at $SERVER_SIGNED_CERT_PATH."
    fi

    # Step 6: Create the server certificate chain
    if [ -f "$SERVER_SIGNED_CERT_PATH" ] && [ -f "$CA_CERT_PATH" ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Creating server certificate chain for $NODE..."
      cat "$SERVER_SIGNED_CERT_PATH" "$CA_CERT_PATH" > "$SERVER_CERT_CHAIN_PATH"
      handle_exit

      # Verify that the certificate chain file was created successfully
      if [ -f "$SERVER_CERT_CHAIN_PATH" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Server certificate chain successfully created at $SERVER_CERT_CHAIN_PATH."
      else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Server certificate chain was not created. Exiting."
        exit 1
      fi
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Signed server certificate or CA certificate not found. Cannot create server certificate chain."
      exit 1
    fi

    # Step 7: Import the signed server certificate into the keystore
    keytool -importcert -alias "$SERVER_ALIAS" -file "$SERVER_CERT_CHAIN_PATH" -keystore "$SERVER_KEYSTORE_PATH" \
      -storepass "$KEYSTORE_PASSWORD" -storetype PKCS12 -noprompt -v
    handle_exit

    echo "$(date '+%Y-%m-%d %H:%M:%S') - Importing CA certificate into the server keystore for $SERVER_ALIAS."
    keytool -importcert -alias "rootCA" -file "$CA_CERT_PATH" -keystore "$SERVER_KEYSTORE_PATH" \
      -storepass "$KEYSTORE_PASSWORD" -storetype PKCS12 -noprompt -v
    handle_exit

    ############################
    # Generate Client Certificate
    ############################
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating client keystore and certificates for $NODE..."

    # Define Distinguished Name for client
    CLIENT_DNAME=$(generate_dname "$CLIENT_ALIAS")

    # Step 3: Verify and create client alias in client keystore
    verify_and_create_alias "$CLIENT_ALIAS" "$CLIENT_KEYSTORE_PATH" "$KEYSTORE_PASSWORD" "$CLIENT_DNAME" "keystore" "$IPV4"
    handle_exit

    # Step 4: Create the client CSR if not already created or invalid, including SANs
    if [ ! -f "$CLIENT_CSR_PATH" ] || ! validate_csr "$CLIENT_CSR_PATH"; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating CSR for client alias $CLIENT_ALIAS..."
      # Updated SAN syntax to lowercase 'san'
      keytool -certreq -alias "$CLIENT_ALIAS" -file "$CLIENT_CSR_PATH" -keystore "$CLIENT_KEYSTORE_PATH" -storepass "$KEYSTORE_PASSWORD" \
        -storetype PKCS12 -dname "$CLIENT_DNAME" -noprompt -v \
        -ext "san=dns:$CLIENT_ALIAS,ip:$IPV4" -rfc
      handle_exit
      sleep 5

      # Verify CSR format
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Verifying the client CSR format..."
      if validate_csr "$CLIENT_CSR_PATH"; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Client CSR is valid."
      else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Client CSR is invalid after regeneration."
        exit 1
      fi
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Client CSR already exists and is valid at $CLIENT_CSR_PATH. Proceeding with signing."
    fi

    # Step 5: Generate the signed client certificate if it does not exist
    if [ ! -f "$CLIENT_SIGNED_CERT_PATH" ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Signing the client CSR to create the signed client certificate..."

      EXTFILE=$(mktemp)  # Create a temporary file for the extension

      # Construct the SAN entries for client
      SAN_ENTRIES="[alt_names]
DNS.1 = ${CLIENT_ALIAS}
IP.1 = ${IPV4}"

      cat <<EOF > "$EXTFILE"
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]
commonName = ${CLIENT_ALIAS}

[v3_req]
subjectAltName = @alt_names
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth

$SAN_ENTRIES
EOF
      sleep 5

      # Sign the CSR using EC key with SHA384
      openssl x509 -req -in "$CLIENT_CSR_PATH" -CA "$CA_CERT_PATH" -CAkey "$CA_KEY_PATH" -CAcreateserial \
        -out "$CLIENT_SIGNED_CERT_PATH" -days 3650 -passin pass:"$PEM_PASSPHRASE" \
        -extensions v3_req -extfile "$EXTFILE" -sha384
      handle_exit
      sleep 5

      # Clean up the temporary extension file after use
      rm -f "$EXTFILE"
      handle_exit
      sleep 5

      echo "$(date '+%Y-%m-%d %H:%M:%S') - Signed client certificate created at $CLIENT_SIGNED_CERT_PATH"
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Signed client certificate already exists at $CLIENT_SIGNED_CERT_PATH."
    fi

    # Step 6: Create the client certificate chain
    if [ -f "$CLIENT_SIGNED_CERT_PATH" ] && [ -f "$CA_CERT_PATH" ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Creating client certificate chain for $NODE..."
      cat "$CLIENT_SIGNED_CERT_PATH" "$CA_CERT_PATH" > "$CLIENT_CERT_CHAIN_PATH"
      handle_exit

      # Verify that the certificate chain file was created successfully
      if [ -f "$CLIENT_CERT_CHAIN_PATH" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Client certificate chain successfully created at $CLIENT_CERT_CHAIN_PATH."
      else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Client certificate chain was not created. Exiting."
        exit 1
      fi
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Signed client certificate or CA certificate not found. Cannot create client certificate chain."
      exit 1
    fi

    # Step 7: Import the signed client certificate into the client keystore
    keytool -importcert -alias "$CLIENT_ALIAS" -file "$CLIENT_CERT_CHAIN_PATH" -keystore "$CLIENT_KEYSTORE_PATH" \
      -storepass "$KEYSTORE_PASSWORD" -storetype PKCS12 -noprompt -v
    handle_exit

    echo "$(date '+%Y-%m-%d %H:%M:%S') - Importing CA certificate into the client keystore for $CLIENT_ALIAS."
    keytool -importcert -alias "rootCA" -file "$CA_CERT_PATH" -keystore "$CLIENT_KEYSTORE_PATH" \
      -storepass "$KEYSTORE_PASSWORD" -storetype PKCS12 -noprompt -v
    handle_exit

    ############################
    # Generate Client Certificate Completed
    ############################
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Keystore and certificates for $CLIENT_ALIAS generated successfully."
    echo "---------------------------------------------------------------"
  done

  ############################
  # Generate Admin Keystore and Certificate
  ############################
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating keystore and certificates for $ADMIN_NAME..."

  # Define paths for admin's certificates
  ADMIN_KEYSTORE_PATH="/conf/ssl/${ADMIN_NAME}.keystore.p12"
  ADMIN_CSR_PATH="/conf/ssl/${ADMIN_NAME}.csr"
  ADMIN_SIGNED_CERT_PATH="/conf/ssl/${ADMIN_NAME}-signed.cert"
  ADMIN_CERT_CHAIN_PATH="/conf/ssl/${ADMIN_NAME}-cert-chain.pem"

  # Generate dynamic DNAME for admin
  ADMIN_DNAME=$(generate_dname "$ADMIN_NAME")

  # Step 1: Verify and create admin alias in keystore
  # Since admin does not have an associated IP, SAN will only include DNS
  verify_and_create_alias "$ADMIN_NAME" "$ADMIN_KEYSTORE_PATH" "$KEYSTORE_PASSWORD" "$ADMIN_DNAME" "keystore"
  handle_exit

  # Step 2: Create the admin CSR if not already created or invalid, without IP in SAN
  if [ ! -f "$ADMIN_CSR_PATH" ] || ! validate_csr "$ADMIN_CSR_PATH"; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating CSR for admin alias $ADMIN_NAME..."
    keytool -certreq -alias "$ADMIN_NAME" -file "$ADMIN_CSR_PATH" -keystore "$ADMIN_KEYSTORE_PATH" -storepass "$KEYSTORE_PASSWORD" \
      -storetype PKCS12 -dname "$ADMIN_DNAME" -noprompt -v \
      -ext "san=dns:$ADMIN_NAME" -rfc
    handle_exit
    sleep 5

    # Verify CSR format
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Verifying the admin CSR format..."
    if validate_csr "$ADMIN_CSR_PATH"; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Admin CSR is valid."
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Admin CSR is invalid after regeneration."
      exit 1
    fi
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Admin CSR already exists and is valid at $ADMIN_CSR_PATH. Proceeding with signing."
  fi

  # Step 3: Generate the signed admin certificate if it does not exist
  if [ ! -f "$ADMIN_SIGNED_CERT_PATH" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Signing the admin CSR to create the signed admin certificate..."

    EXTFILE=$(mktemp)  # Create a temporary file for the extension

    # Construct the SAN entries for admin (only DNS, no IP)
    SAN_ENTRIES="[alt_names]
DNS.1 = ${ADMIN_NAME}"

    cat <<EOF > "$EXTFILE"
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]
commonName = ${ADMIN_NAME}

[v3_req]
subjectAltName = @alt_names
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth

$SAN_ENTRIES
EOF
    sleep 5

    # Sign the CSR using EC key with SHA384
    openssl x509 -req -in "$ADMIN_CSR_PATH" -CA "$CA_CERT_PATH" -CAkey "$CA_KEY_PATH" -CAcreateserial \
      -out "$ADMIN_SIGNED_CERT_PATH" -days 3650 -passin pass:"$PEM_PASSPHRASE" \
      -extensions v3_req -extfile "$EXTFILE" -sha384
    handle_exit
    sleep 5

    # Clean up the temporary extension file after use
    rm -f "$EXTFILE"
    handle_exit
    sleep 5

    echo "$(date '+%Y-%m-%d %H:%M:%S') - Signed admin certificate created at $ADMIN_SIGNED_CERT_PATH"
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Signed admin certificate already exists at $ADMIN_SIGNED_CERT_PATH."
  fi

  # Step 4: Create the admin certificate chain
  if [ -f "$ADMIN_SIGNED_CERT_PATH" ] && [ -f "$CA_CERT_PATH" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Creating admin certificate chain..."
    cat "$ADMIN_SIGNED_CERT_PATH" "$CA_CERT_PATH" > "$ADMIN_CERT_CHAIN_PATH"
    handle_exit

    # Verify that the certificate chain file was created successfully
    if [ -f "$ADMIN_CERT_CHAIN_PATH" ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Admin certificate chain successfully created at $ADMIN_CERT_CHAIN_PATH."
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Admin certificate chain was not created. Exiting."
      exit 1
    fi
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Signed admin certificate or CA certificate not found. Cannot create admin certificate chain."
    exit 1
  fi

  # Step 5: Import the signed admin certificate into the keystore
  keytool -importcert -alias "$ADMIN_NAME" -file "$ADMIN_CERT_CHAIN_PATH" -keystore "$ADMIN_KEYSTORE_PATH" \
    -storepass "$KEYSTORE_PASSWORD" -storetype PKCS12 -noprompt -v
  handle_exit

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Importing CA certificate into the admin keystore for $ADMIN_NAME."
  # Add a check to see if the alias "rootCA" already exists
  if ! keytool -list -alias "rootCA" -keystore "$ADMIN_KEYSTORE_PATH" -storepass "$KEYSTORE_PASSWORD" -storetype PKCS12 >/dev/null 2>&1; then
      keytool -importcert -alias "rootCA" -file "$CA_CERT_PATH" -keystore "$ADMIN_KEYSTORE_PATH" \
          -storepass "$KEYSTORE_PASSWORD" -storetype PKCS12 -noprompt -v
      handle_exit
  else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Alias rootCA already exists in admin keystore. Skipping import."
  fi

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Keystore and certificates for $ADMIN_NAME generated successfully."
  echo "---------------------------------------------------------------"
}

############################
# Generate Admin Client Keystore and Certificate
############################
generate_admin_client_keystore() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating keystore and certificates for admin client..."

  # Define paths for admin's client certificates
  ADMIN_CLIENT_ALIAS="admin-client"
  ADMIN_CLIENT_KEYSTORE_PATH="/conf/ssl/admin-client.keystore.p12"
  ADMIN_CLIENT_CSR_PATH="/conf/ssl/admin-client.csr"
  ADMIN_CLIENT_SIGNED_CERT_PATH="/conf/ssl/admin-client-signed.cert"
  ADMIN_CLIENT_CERT_CHAIN_PATH="/conf/ssl/admin-client-cert-chain.pem"
  CA_CERT_PATH="/conf/ssl/ca-cert.crt"
  CA_KEY_PATH="/conf/ssl/ca-key.key"

  # Define Distinguished Name for admin client
  ADMIN_CLIENT_DNAME=$(generate_dname "$ADMIN_CLIENT_ALIAS")

  # Verify and create admin client alias in keystore
  verify_and_create_alias "$ADMIN_CLIENT_ALIAS" "$ADMIN_CLIENT_KEYSTORE_PATH" "$KEYSTORE_PASSWORD" "$ADMIN_CLIENT_DNAME" "keystore"
  handle_exit

  # Generate the CSR for the client if it does not already exist or is invalid
  if [ ! -f "$ADMIN_CLIENT_CSR_PATH" ] || ! validate_csr "$ADMIN_CLIENT_CSR_PATH"; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Generating CSR for admin client alias $ADMIN_CLIENT_ALIAS..."
    keytool -certreq -alias "$ADMIN_CLIENT_ALIAS" -file "$ADMIN_CLIENT_CSR_PATH" -keystore "$ADMIN_CLIENT_KEYSTORE_PATH" \
      -storepass "$KEYSTORE_PASSWORD" -dname "$ADMIN_CLIENT_DNAME" -ext "san=dns:$ADMIN_CLIENT_ALIAS" -rfc
    handle_exit
    sleep 5
  fi

  # Sign the CSR for admin client and create the certificate chain
  if [ -f "$ADMIN_CLIENT_CSR_PATH" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Signing the admin client CSR..."
    openssl x509 -req -in "$ADMIN_CLIENT_CSR_PATH" -CA "$CA_CERT_PATH" -CAkey "$CA_KEY_PATH" -CAcreateserial \
      -out "$ADMIN_CLIENT_SIGNED_CERT_PATH" -days 3650 -passin pass:"$PEM_PASSPHRASE" -extfile <(echo "subjectAltName=DNS:$ADMIN_CLIENT_ALIAS")
    handle_exit

    # Create the admin client certificate chain
    cat "$ADMIN_CLIENT_SIGNED_CERT_PATH" "$CA_CERT_PATH" > "$ADMIN_CLIENT_CERT_CHAIN_PATH"
    handle_exit
  fi

  # Import the signed admin client certificate into the keystore
  keytool -importcert -alias "$ADMIN_CLIENT_ALIAS" -file "$ADMIN_CLIENT_CERT_CHAIN_PATH" -keystore "$ADMIN_CLIENT_KEYSTORE_PATH" \
    -storepass "$KEYSTORE_PASSWORD" -noprompt
  handle_exit

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Keystore and certificates for admin client generated successfully."
}

# Phase 2: Truststore Population
populate_truststores() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting Phase 2: Truststore Population"

  for NODE in $ALL_NODES; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Populating truststore for $NODE..."

    TRUSTSTORE_PATH="/conf/ssl/${NODE}.truststore.p12"

    # Step 1: Import CA certificate into the truststore if not already present
    if keytool -list -alias rootCA -keystore "$TRUSTSTORE_PATH" -storepass "$TRUSTSTORE_PASSWORD" -storetype PKCS12 >/dev/null 2>&1; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - CA certificate already exists in ${NODE}'s truststore. Skipping import."
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Importing CA certificate into ${NODE}'s truststore."
      keytool -importcert -alias rootCA -file "$CA_CERT_PATH" -keystore "$TRUSTSTORE_PATH" \
        -storepass "$TRUSTSTORE_PASSWORD" -storetype PKCS12 -noprompt -v
      handle_exit
    fi 

    echo "$(date '+%Y-%m-%d %H:%M:%S') - Truststore for $NODE populated successfully."
    echo "---------------------------------------------------------------"
  done

  # **Add Admin Truststore Population**
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Populating truststore for admin..."

  ADMIN_TRUSTSTORE_PATH="/conf/ssl/${ADMIN_NAME}.truststore.p12"

  if keytool -list -alias rootCA -keystore "$ADMIN_TRUSTSTORE_PATH" -storepass "$TRUSTSTORE_PASSWORD" -storetype PKCS12 >/dev/null 2>&1; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - CA certificate already exists in admin's truststore. Skipping import."
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Importing CA certificate into admin's truststore."
    keytool -importcert -alias rootCA -file "$CA_CERT_PATH" -keystore "$ADMIN_TRUSTSTORE_PATH" \
      -storepass "$TRUSTSTORE_PASSWORD" -storetype PKCS12 -noprompt -v
    handle_exit
  fi 

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Truststore for admin populated successfully."
  echo "---------------------------------------------------------------"

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Phase 2: Truststore Population Completed."
}

# Function to import certificates into truststore if the alias does not already exist
import_certificate_if_not_exists() {
  ALIAS=$1
  CERT_FILE=$2
  TRUSTSTORE=$3
  TRUSTSTORE_PASSWORD=$4

  if keytool -list -alias "$ALIAS" -keystore "$TRUSTSTORE" -storepass "$TRUSTSTORE_PASSWORD" -storetype PKCS12 >/dev/null 2>&1; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Alias $ALIAS already exists in the truststore. Skipping import."
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Importing certificate for alias $ALIAS into the truststore."
    keytool -importcert -alias "$ALIAS" -file "$CERT_FILE" -keystore "$TRUSTSTORE" -storepass "$TRUSTSTORE_PASSWORD" -storetype PKCS12 -noprompt -v
    handle_exit
  fi
}

# Function to generate keystores and manage certificates for all nodes and the admin
generate_keystores_and_certificates() {
  generate_certificates
  populate_truststores
}

# Execute the function to generate keystores and certificates for all nodes and the admin
generate_keystores_and_certificates

# Generate admin client keystore and certificate
generate_admin_client_keystore

# Step 3: Extract client keys after all certificates are generated
extract_client_keys() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting Step 3: Extract Client Keys"

  for CLIENT_NODE in $ALL_NODES $ADMIN_NAME; do
    CLIENT_KEYSTORE_PATH="/conf/ssl/${CLIENT_NODE}-client.keystore.p12"
    CLIENT_KEY_OUTPUT="/conf/ssl/${CLIENT_NODE}-client.key"

    # Check if the keystore exists before attempting extraction
    if [ -f "$CLIENT_KEYSTORE_PATH" ]; then
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Extracting client key for $CLIENT_NODE..."
      # Extract the client key using the passphrase dynamically
      openssl pkcs12 \
        -in "$CLIENT_KEYSTORE_PATH" \
        -nocerts -nodes \
        -out "$CLIENT_KEY_OUTPUT" \
        -passin pass:"$KEYSTORE_PASSWORD"
      handle_exit
      
      # Secure the extracted client key
      chmod 600 "$CLIENT_KEY_OUTPUT"
      handle_exit

      echo "Extracted client key for $CLIENT_NODE at $CLIENT_KEY_OUTPUT"
    else
      echo "$(date '+%Y-%m-%d %H:%M:%S') - Keystore not found for $CLIENT_NODE. Skipping key extraction."
    fi
  done

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Step 3: Extract Client Keys Completed."
}

# Execute the client keys extraction
extract_client_keys

echo "$(date '+%Y-%m-%d %H:%M:%S') - Script completed successfully."
