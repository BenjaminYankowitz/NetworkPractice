Set up cmake with
cmake -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake -B build -G Ninja -DCMAKE_EXPORT_COMPILE_COMMANDS=1
