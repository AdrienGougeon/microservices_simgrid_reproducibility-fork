set(Boost_USE_STATIC_LIBS        ON)
find_package(Boost REQUIRED COMPONENTS json)

add_library(Elasticity ElasticTask.cpp DataSource.cpp ElasticPolicyCPUThreshold.cpp ElasticPolicyHybrid1.cpp ElasticPolicyReactive1.cpp TaskInstance.cpp)
target_link_libraries(Elasticity 
  SimGrid::SimGrid
  ${OPENTELEMETRY_CPP_LIBRARIES}
  Boost::json
  )
TARGET_LINK_LIBRARIES(Elasticity ${EXTRA_LIBS})
