#!/bin/bash
set -e

echo "🔨 Building Strimzi Stretch MCS Plugin"
echo "======================================="

# Check if Strimzi is available
STRIMZI_PATH="${STRIMZI_PATH:-../strimzi-kafka-operator}"

if [ ! -d "$STRIMZI_PATH" ]; then
    echo "❌ Error: Strimzi not found at $STRIMZI_PATH"
    echo "   Set STRIMZI_PATH environment variable or clone Strimzi to ../strimzi-kafka-operator"
    exit 1
fi

echo ""
echo "📦 Step 1: Installing Strimzi SPI to local Maven repo..."
cd "$STRIMZI_PATH"
echo "   (Compiling source code...)"
# Compile (skip checkstyle due to pre-existing issues in operator code)
mvn clean package -pl cluster-operator -Dmaven.test.skip=true -Dcheckstyle.skip=true -q
if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to compile Strimzi cluster-operator"
    echo "   Running with verbose output to see the error..."
    mvn clean package -pl cluster-operator -Dmaven.test.skip=true
    exit 1
fi

# Install the JAR to local Maven repo
mvn install:install-file \
  -Dfile=cluster-operator/target/cluster-operator-0.48.0.jar \
  -DgroupId=io.strimzi \
  -DartifactId=cluster-operator \
  -Dversion=0.48.0 \
  -Dpackaging=jar \
  -q

echo "✅ Strimzi SPI installed"

echo ""
echo "🔨 Step 2: Building MCS Plugin..."
cd - > /dev/null
mvn clean package -q
echo "✅ Plugin built successfully"

echo ""
echo "📋 Step 3: Verifying JAR contents..."
JAR_FILE=$(ls target/strimzi-stretch-mcs-plugin-*.jar 2>/dev/null | head -1)
if [ -z "$JAR_FILE" ]; then
    echo "❌ Error: JAR file not found"
    exit 1
fi

echo "   JAR: $JAR_FILE"
echo "   Size: $(du -h "$JAR_FILE" | cut -f1)"
echo ""
echo "   Contents:"
jar tf "$JAR_FILE" | grep -E "(McsNetworkingProvider|META-INF)" | head -10

echo ""
echo "✅ Build complete!"
echo ""
echo "📦 Plugin JAR: $JAR_FILE"
echo ""
echo "Next steps:"
echo "  1. Create ConfigMap: kubectl create configmap strimzi-stretch-mcs-plugin --from-file=$JAR_FILE -n strimzi"
echo "  2. Update operator deployment (see README.md)"
echo "  3. Restart operator: kubectl rollout restart deployment/strimzi-cluster-operator -n strimzi"
echo ""
