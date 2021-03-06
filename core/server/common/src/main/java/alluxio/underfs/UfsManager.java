/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs;

import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;

import java.io.Closeable;

/**
 * A class that manages the UFS used by different services.
 */
public interface UfsManager extends Closeable {
  /**
   * Maps a mount id to a UFS instance. Based on the UFS uri and conf, if this UFS instance already
   * exists in the cache, map the mount id to this existing instance. Otherwise, creates a new
   * instance and adds that to the cache. Use this method only when you create new UFS instances.
   *
   * @param mountId the mount id
   * @param ufsUri the UFS path
   * @param ufsConf the UFS configuration
   * @return the UFS instance
   */
  UnderFileSystem addMount(long mountId, String ufsUri, UnderFileSystemConfiguration ufsConf);

  /**
   * Removes the association from a mount id to a UFS instance. If the mount id is not known, this
   * is a noop.
   *
   * @param mountId the mount id
   *
   */
  void removeMount(long mountId);

  /**
   * Gets a UFS instance from the cache if exists, or throws exception otherwise.
   *
   * @param mountId the mount id
   * @return the UFS instance
   * @throws NotFoundException if mount id is not found in mount table
   * @throws UnavailableException if master is not available to query for mount table
   */
  UnderFileSystem get(long mountId) throws NotFoundException, UnavailableException;

  /**
   * @return the UFS instance associated with root
   */
  UnderFileSystem getRoot();
}
