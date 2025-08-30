package it.costantino.astaapp.service.impl;

import it.costantino.astaapp.domain.WatchList;
import it.costantino.astaapp.repository.WatchListRepository;
import it.costantino.astaapp.service.WatchListService;
import it.costantino.astaapp.service.dto.WatchListDTO;
import it.costantino.astaapp.service.mapper.WatchListMapper;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service Implementation for managing {@link it.costantino.astaapp.domain.WatchList}.
 */
@Service
@Transactional
public class WatchListServiceImpl implements WatchListService {

    private static final Logger LOG = LoggerFactory.getLogger(WatchListServiceImpl.class);

    private final WatchListRepository watchListRepository;

    private final WatchListMapper watchListMapper;

    public WatchListServiceImpl(WatchListRepository watchListRepository, WatchListMapper watchListMapper) {
        this.watchListRepository = watchListRepository;
        this.watchListMapper = watchListMapper;
    }

    @Override
    public WatchListDTO save(WatchListDTO watchListDTO) {
        LOG.debug("Request to save WatchList : {}", watchListDTO);
        WatchList watchList = watchListMapper.toEntity(watchListDTO);
        watchList = watchListRepository.save(watchList);
        return watchListMapper.toDto(watchList);
    }

    @Override
    public WatchListDTO update(WatchListDTO watchListDTO) {
        LOG.debug("Request to update WatchList : {}", watchListDTO);
        WatchList watchList = watchListMapper.toEntity(watchListDTO);
        watchList = watchListRepository.save(watchList);
        return watchListMapper.toDto(watchList);
    }

    @Override
    public Optional<WatchListDTO> partialUpdate(WatchListDTO watchListDTO) {
        LOG.debug("Request to partially update WatchList : {}", watchListDTO);

        return watchListRepository
            .findById(watchListDTO.getId())
            .map(existingWatchList -> {
                watchListMapper.partialUpdate(existingWatchList, watchListDTO);

                return existingWatchList;
            })
            .map(watchListRepository::save)
            .map(watchListMapper::toDto);
    }

    @Override
    @Transactional(readOnly = true)
    public List<WatchListDTO> findAll() {
        LOG.debug("Request to get all WatchLists");
        return watchListRepository.findAll().stream().map(watchListMapper::toDto).collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<WatchListDTO> findOne(Long id) {
        LOG.debug("Request to get WatchList : {}", id);
        return watchListRepository.findById(id).map(watchListMapper::toDto);
    }

    @Override
    public void delete(Long id) {
        LOG.debug("Request to delete WatchList : {}", id);
        watchListRepository.deleteById(id);
    }
}
